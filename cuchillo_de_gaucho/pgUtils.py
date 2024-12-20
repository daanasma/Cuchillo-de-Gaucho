from sqlalchemy import Engine, create_engine, text

import logging

from typing import Union, List


def make_connection_string_postgres(
        db_name: str,
        user: str,
        password: str,
        host: str,
        port: int = 5432
) -> str:
    """
    Creates a PostgreSQL/PostGIS connection string for use with ogr2ogr or similar tools.

    :param db_name: Name of the PostgreSQL/PostGIS database
    :param user: PostgreSQL username
    :param password: PostgreSQL password
    :param host: PostgreSQL host (default: 'localhost')
    :param port: PostgreSQL port (default: 5432)
    :return: A formatted connection string for PostgreSQL/PostGIS
    """
    connection_string = f'''PG:dbname='{db_name}' host='{host}' port='{port}' user='{user}' password='{password}' '''

    print(connection_string)
    return connection_string

def connect_postgres_database(
    user: str, password: str, host: str, port:str, dbname: str
) -> Engine:
    """
    Connect to an existing postgres database

    :param user: The server user
    :param password: The password for the user
    :param host: The hostname, excluding port (eg localhost)
    :param host: The port (eg 5432)
    :param dbname: The name f the database to be created
    :return: A sqlalchemy engine for the database
    """
    e = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )
    logging.info(f"Succesfullly created engine to database {dbname}")
    return e


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
        # Execute all queries in the list
        for query in q:
            query_obj = text(query)
            logging.debug(f"Executing query: {query}")
            connection.execute(query_obj)

        # Commit transaction if all queries succeed
        trans.commit()
        logging.info("All queries executed successfully and transaction committed.")

    except Exception as e:
        # Rollback transaction in case of an error
        trans.rollback()
        logging.error(f"Error executing queries: {e}")
        raise

    finally:
        # Ensure that the connection is properly closed
        connection.close()
        logging.info("Connection closed.")

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