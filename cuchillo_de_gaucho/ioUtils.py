from . import winUtils as wu
from . import geoUtils as gu
from . import pathUtils as pu
from . import pgUtils as pgu
import os
import geopandas as gpd
import pandas as pd
import polars as pl
from .decorators import time_function

from sqlalchemy import Engine

import logging

#READING
def read_file_to_geodataframe(path: str, driver: str = "ESRI Shapefile") -> gpd.GeoDataFrame:
	"""
	Reads a spatial dataset from a path. The default driver is shapefile, but others can be specified.

	:param path: The path to the file
	:param epsg: The source crs of the spatial file
	:param driver: The driver to be used (fiona.supported_drivers)
	:returns: A geodataframe representing the file
	"""

	layername = os.path.basename(path)
	foldername = os.path.dirname(path)
	logging.info(f"reading spatial dataframe {layername} from {foldername}")
	if driver == 'GPKG':
		gdf = gpd.read_file(foldername, layer=layername)
	else:
		gdf = gpd.read_file(path, driver=driver)
	logging.info(f"Finished reading spatial dataframe. size = {len(gdf)}")
	return gdf


import os
import pandas as pd
import logging

import os
import pandas as pd
import logging


def read_csv_to_dataframe(path: str, delimiter: str = ",", dtypes: dict = None) -> pd.DataFrame:
	"""
	Reads a CSV file from a given path into a pandas DataFrame. The default delimiter is a comma,
	but others can be specified. Specific data types for columns can be defined using a dictionary.

	:param path: The path to the CSV file
	:param delimiter: The delimiter used in the CSV file (default: ',')
	:param dtypes: A dictionary mapping column names to data types (e.g., {"col1": "int64", "col2": "float64"})
		For a full list of supported data types, refer to the pandas documentation:
		https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
	:returns: A pandas DataFrame containing the data from the CSV file
	"""

	filename = os.path.basename(path)
	foldername = os.path.dirname(path)
	logging.info(f"Reading CSV file '{filename}' from folder '{foldername}'")

	try:
		df = pd.read_csv(path, delimiter=delimiter, dtype=dtypes)
		logging.info(f"Finished reading CSV file. Number of rows = {len(df)}")
		return df
	except Exception as e:
		logging.error(f"Failed to read CSV file '{filename}'. Error: {e}")
		raise


def read_postgres_to_pandas_df(query: str, engine: Engine) -> pd.DataFrame:
    """
    Reads data from a PostgreSQL database into a Pandas DataFrame using an SQLAlchemy engine.

    Parameters:
        query (str): The SQL query to execute.
        engine (Engine): SQLAlchemy engine connected to the database.

    Returns:
        pd.DataFrame: Data fetched from the database as a Pandas DataFrame.
    """
    try:
        # Use the engine to execute the query and load data into a Pandas DataFrame
        df = pd.read_sql(query, con=engine)
        logging.info("Query executed and data loaded into Pandas DataFrame.")
        return df
    except Exception as e:
        logging.error(f"Error while fetching data: {e}")
        raise



#WRITING
@time_function
def write_pandas_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str, engine: Engine, if_exists: str = 'replace') -> None:
    """
    Writes data from a Pandas DataFrame to a PostgreSQL table using an SQLAlchemy engine.

    Parameters:
        df (pd.DataFrame): The Pandas DataFrame to be written to the database.
        schema_name (str): The name of the table to write the data to. (no schema here)
        table_name (str): The name of the table to write the data to.
        engine (Engine): SQLAlchemy engine connected to the database.
        if_exists (str): Action to take if the table already exists.
                          Options: 'replace', 'append', 'fail'. Default is 'replace'.
    """

    try:
        # Use the engine to write the DataFrame to the PostgreSQL database
        df.to_sql(table_name, con=engine, schema=schema_name, if_exists=if_exists, index=False, method='multi')
        logging.info(f"Data successfully written to {table_name} in the database.")
    except Exception as e:
        logging.error(f"Error while writing data to {table_name}: {e}")
        raise


def write_from_geodataframe(gdf: gpd.GeoDataFrame, foldername: str, layername: str, driver: str = "ESRI Shapefile"):
	"""
	Writes a geodataframe to a spatial dataset at a specified path. If the path does not exist, it is created

	:returns:
	"""
	logging.info(f"writing spatial dataframe {layername} to {foldername}")

	if not foldername.endswith('.gpkg'):
		pu.create_folder_if_not_exists(foldername)

	if driver == 'GPKG':
		gdf.to_file(foldername, driver=driver, layer=layername)
	else:
		gdf.to_file(filename=os.path.join(foldername, layername), driver=driver)

	logging.info("Successfully wrote spatial dataframe")


def ogr_load_data_to_geopackage(ogr_path, geopackage_path, source, lr_name, overwrite=True):
	features_in_geopackage_path = []
	if not overwrite and os.path.exists(geopackage_path):
		features_in_geopackage_path = gu.list_all_features_in_geopackage_sqlite(geopackage_path)

	if (not overwrite and lr_name in features_in_geopackage_path):
		logging.warning(f"Not loading {lr_name} because it already exists in target geopackage")
	else:
		ogr2ogr_command = [
			ogr_path,  # Path to ogr2ogr
			"-f", "GPKG",  # Output format
			"-update",  # Allow updates to the file
			"-overwrite",  # Append data
			#"-append",  # Append data
			geopackage_path,  # Output GeoPackage
			source,  # Input Shapefile
			"-nln", lr_name, # Layer name in the GeoPackage
			"-lco", "SPATIAL_INDEX=YES"
		]
		logging.info(f"Start {ogr2ogr_command}")
		wu.run_subprocess(ogr2ogr_command)


def ogr_load_data_to_postgis(
		ogr_path,
		source_path,
		db_name,
		user,
		password,
		host,
		port=5432,
		source_layer_name=None,
		target_table_name=None,
		schema_name='public',
		overwrite=True,
		source_format=None,
):
	"""
	Load a spatial dataset into PostGIS using ogr2ogr, supports multiple input file types.

	:param source_path: Path to the input spatial file (e.g., GeoPackage, Shapefile, GeoJSON)
	:param db_name: PostGIS database name
	:param user: PostgreSQL username
	:param password: PostgreSQL password
	:param host: PostgreSQL host (default: localhost)
	:param port: PostgreSQL port (default: 5432)
	:param source_name: Name of the layer to be imported. If None, imports the default layer.
	:param target_table_name: Target table name in PostGIS (optional, defaults to source layer name)
	:param schema: Target schema in PostGIS (optional, defaults to public)
	:param overwrite: If True, overwrite the target table if it exists
	:param source_format: Input file format (e.g., 'GPKG', 'ESRI Shapefile'). Auto-detected if None.
	:return: None
	"""
	logging.info("Start loading data to postgres.")
	# Detect the source format if not specified
	if not source_format:
		_, ext = os.path.splitext(source_path)
		ext = ext.lower()
		if ext == '.gpkg':
			source_format = 'GPKG'
		elif ext == '.shp':
			source_format = 'ESRI Shapefile'
		elif ext == '.geojson':
			source_format = 'GeoJSON'
		else:
			raise ValueError(f"Unsupported file extension: {ext}. Please specify the source_format.")

	# Build the connection string for PostgreSQL/PostGIS
	connection_str = pgu.make_connection_string_postgres(db_name, user, password, host, port)

	# Determine layer name if not provided
	if not target_table_name:
		target_table_name = os.path.splitext(os.path.basename(source_path))[0]  # Default table name from file name

	target_name = f"{schema_name}.{target_table_name}"
	# Build the ogr2ogr command
	command = [
		ogr_path,
		'-f', "PostgreSQL",  # Target format (PostgreSQL in this case)
		connection_str,  # Target database connection string
		source_path,  # Input source file
		'-nln', target_name  # Specify target table name
	]

	# Add optional overwrite flag
	if overwrite:
		command.append('-overwrite')

	# Specify the source layer if provided
	if source_layer_name:
		command.append(source_layer_name)

	logging.info(command)
	# Run the command as a subprocess
	wu.run_subprocess(command)


###TRANSFORM

@time_function
def pandas_to_polars(pandas_df):
	"""
	Converts a Pandas DataFrame to a Polars DataFrame.

	Parameters:
		pandas_df (pandas.DataFrame): Input Pandas DataFrame.

	Returns:
		polars.DataFrame: Converted Polars DataFrame.
	"""
	logging.info(f"Start converting pandas to polars. n={len(pandas_df)} rows.")
	pld = pl.from_pandas(pandas_df)
	return pld


@time_function
def polars_to_pandas(polars_df):
    """
    Converts a Polars DataFrame to a Pandas DataFrame.

    Parameters:
        polars_df (polars.DataFrame): Input Polars DataFrame.

    Returns:
        pandas.DataFrame: Converted Pandas DataFrame.
    """
    logging.info(f"Start converting polars to pandas. n={len(polars_df)} rows.")
    pdf = polars_df.to_pandas()
    return pdf
