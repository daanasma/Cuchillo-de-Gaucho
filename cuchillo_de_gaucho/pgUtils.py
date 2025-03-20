from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine.url import make_url
from .decorators import time_function
import logging
import re, os
from typing import Union, List

def make_connection_string_postgres( db_name: str, user: str, password: str, host: str, port: int = 5432, dialect='ogr2ogr') -> str:
    """
    Creates a PostgreSQL/PostGIS connection string for use with ogr2ogr or similar tools.

    :param db_name: Name of the PostgreSQL/PostGIS database
    :param user: PostgreSQL username
    :param password: PostgreSQL password
    :param host: PostgreSQL host (default: 'localhost')
    :param port: PostgreSQL port (default: 5432)
    :param dialect: Dialect to create the postgres connection string
    :return: A formatted connection string for PostgreSQL/PostGIS
    """
    if dialect == 'ogr2ogr':
        connection_string = f'''PG:dbname='{db_name}' host='{host}' port='{port}' user='{user}' password='{password}' '''
    elif dialect == 'sqlalchemy':
        connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    elif dialect == 'connectorx':
        connection_string = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    return connection_string

def connect_postgres_database(user: str, password: str, host: str, port:str, dbname: str) -> Engine:
    """
    Connect to an existing postgres database

    :param user: The server user
    :param password: The password for the user
    :param host: The hostname, excluding port (eg localhost)
    :param host: The port (eg 5432)
    :param dbname: The name f the database to be created
    :return: A sqlalchemy engine for the database
    """
    conn_string = make_connection_string_postgres(dbname, user, password, host, port, dialect='sqlalchemy')
    e = create_engine(conn_string)
    logging.info(f"Succesfullly created engine to database {dbname} for user {user}.")
    return e

def release_all_active_db_connections(engine):
    db_name = make_url(engine.url).database # Get database name from the connection engine
    query = f"""SELECT pg_terminate_backend(pid)
    FROM pg_stat_activity
    WHERE datname = '{db_name}'
    AND pid <> pg_backend_pid();  -- Prevent killing your own session
    """
    execute_postgres_query(engine, query)
    logging.info(f"Terminated all active sesions on database: {db_name}")
@time_function
def execute_postgres_query(e: Engine, q: Union[str, List[str]]):
    """
    Execute SQL query or a list of SQL queries on a PostgreSQL database.

    :param e: The SQLAlchemy engine object.
    :param q: A string or list of strings representing SQL queries.
    :return: None
    :raises Exception: If a query fails, it raises an exception, and no queries are committed.
    """
    # Ensure `q` is always a list for consistency
    if isinstance(q, str):
        q = [q]

    logging.info(f"Executing {len(q)} queries on PostgreSQL")

    # Create a new connection and begin transaction
    connection = e.connect()
    trans = connection.begin()

    try:
        results = []  # Store results for SELECT queries
        # Execute all queries in the list
        for query in q:
            query_obj = text(query)
            logging.info(f"Executing query: {' '.join(query.split())}".replace('\n', ' ').strip()[:100] + " (cutoff at 100)")
            # Execute the query
            result = connection.execute(query_obj)

            # Fetch results for SELECT queries
            if result.returns_rows:
                results.append(result.fetchall())  # Collect all rows for each SELECT query
            else:
                results.append(None)  # Non-SELECT queries return None

        # Commit transaction if all queries succeed
        trans.commit()
        logging.info("All queries executed successfully and transaction committed.")

        return results  # Return the collected results

    except Exception as e:
        # Rollback transaction in case of an error
        trans.rollback()
        logging.error(f"Error executing queries: {e}")
        raise

    finally:
        # Ensure that the connection is properly closed
        connection.close()
        logging.info("Connection closed.")

def execute_postgres_query_from_file(engine, query_file_path):
    """
    Reads a SQL query from the specified file and executes it.

    Parameters:
    - engine: SQLAlchemy engine or connection
    - query_file_path: Full path to the SQL file
    """
    if not os.path.exists(query_file_path):
        raise FileNotFoundError(f"SQL file not found: {query_file_path}")

    # Read the SQL query from the file
    with open(query_file_path, "r", encoding="utf-8") as sql_file:
        query_string = sql_file.read()

    # Execute the query
    return execute_postgres_query(engine, q=query_string)

def get_table_names_matching_wildcard(engine, schema, wildcard):
    """
    Get all table names from a specified schema that match the wildcard.

    :param engine: SQLAlchemy engine instance connected to the database.
    :param schema: The schema name to search within.
    :param wildcard: The wildcard pattern to match table names.
    :return: List of table names matching the wildcard.
    """
    # Query to find tables with the wildcard
    query = f"""
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = '{schema}'AND tablename LIKE '{wildcard}';
    """
    res = execute_postgres_query(engine, query)
    try:
        if len(res):
            return [r[0] for r in res[0]]
    except Exception as e:
        logging.info(f"Cant return tables based on query because {e}")
        return []

def sql_gen_create_schema_if_not_exists(schema_name: str, owner: str = None, grant_usage: bool = True):
    """
    Create a schema if it does not already exist.

    :param schema_name: Name of the schema to be created.
    :param owner: The owner of the schema. If None, the current user will be the owner.
    :param grant_usage: Whether to grant USAGE privileges on the schema to the public. Default is True.
    :return: The SQL statement to create the schema.
    """
    # Start the base SQL for schema creation
    sql = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    # Add the owner clause if an owner is provided
    if owner:
        sql += f" AUTHORIZATION {owner}"

    # Optionally grant usage privileges
    if grant_usage:
        sql += f"; GRANT USAGE ON SCHEMA {schema_name} TO public;"

    return sql

def sql_gen_create_spatial_index(table_name: str, schema_name: str = 'public', geometry_column: str = 'geom') -> str:
    """
    Generate a SQL query to create a spatial index on a geometry column.

    :param table_name: The name of the table to create the index for
    :param geometry_column: The name of the geometry column (default is 'geom')
    :return: SQL query string to create the spatial index
    """
    # Generate the SQL query to create the spatial index
    index_name = f"{table_name}_{geometry_column}_idx"
    sql_query = f""" 
    CREATE INDEX {index_name} 
    ON {schema_name}.{table_name} 
    USING GIST ({geometry_column}); 
    """
    return sql_query

def sql_gen_convert_wkt_to_geom(table_to_update: str, wkt_col: str, geometry_column: str = 'geom') -> str:
    sql = f"""-- Step 1: Add the new geometry column
        ALTER TABLE {table_to_update}
        ADD COLUMN {geometry_column} geometry;
        
        -- Step 2: Update the new column with the converted WKT values
        UPDATE  {table_to_update}
        SET point_geom = ST_GeomFromText({wkt_col});
        
        -- Step 3: Drop the original WKT column
        ALTER TABLE  {table_to_update}
        DROP COLUMN {wkt_col};
        """
    return sql


def sql_gen_remove_records_from_table(table_to_update: str, field_to_filter: str, values_to_remove: set) -> str:
    """
    Generate a single SQL statement to remove records from a table based on a set of values.
    Uses a temporary table for better performance with large datasets.

    :param table_to_update: Name of the table to update.
    :param field_to_filter: Column name to filter records by.
    :param values_to_remove: Set of values to remove.
    :return: SQL statement as a single string.
    """

    if not values_to_remove:
        return ""  # No values to remove, return empty string

    values_sql = ",\n        ".join(f"('{val}')" for val in values_to_remove)

    sql_statement = f"""
        CREATE TEMP TABLE temp_values_to_remove (value_to_remove TEXT PRIMARY KEY);

        INSERT INTO temp_values_to_remove (value_to_remove) VALUES
        {values_sql};

        DELETE FROM {table_to_update}
        WHERE {field_to_filter} IN (SELECT value_to_remove FROM temp_values_to_remove);

        DROP TABLE temp_values_to_remove;
    """

    return sql_statement
