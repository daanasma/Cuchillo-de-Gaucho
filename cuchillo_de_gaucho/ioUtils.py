from . import winUtils as wu
from . import geoUtils as geou
from . import pathUtils as pu
from . import pgUtils as pgu
from . import packageConfig
import os
import geopandas as gpd
import pandas as pd
import polars as pl
import fiona
from .decorators import time_function

from sqlalchemy import Engine
from geoalchemy2 import Geometry, WKTElement
from shapely.geometry import shape
from shapely import wkt

import logging


# READING
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
	logging.info(f"Finished reading spatial dataframe {layername}. size = {len(gdf)}")
	return gdf


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
		df = pd.read_csv(path, delimiter=delimiter, dtype=dtypes, na_values=['NA', ''])
		logging.info(f"Finished reading CSV file . Number of rows = {len(df)} (path: {path})")
		return df
	except Exception as e:
		logging.error(f"Failed to read CSV file '{filename}'. Error: {e}")
		raise

def read_geoparquet_to_polars(path: str, geometry_field: str = 'geometry', dtype_transform: dict= None):
	"""
	Reads a GeoParquet file into a Polars DataFrame, converting geometries to WKT format.

	:param path:
	    Path to the GeoParquet file.
	:param geometry_field:
	    Name of the geometry column to be converted to WKT format. Defaults to 'geometry'.
	:param dtype_transform:
	    Optional dictionary mapping column names to target data types for conversion.

	:return:
	    A Polars DataFrame with geometries as WKT strings and optional type transformations applied.
	"""
	gdf = gpd.read_parquet(path)
	gdf[geometry_field] = gdf[geometry_field].apply(lambda geom: geom.wkt if geom else None)
	if dtype_transform:
		for fieldname, datatype in dtype_transform.items():
			gdf[fieldname] = gdf[fieldname].astype(datatype)

	return pl.from_pandas(gdf)


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


# WRITING
@time_function
def write_pandas_to_postgres(df: pd.DataFrame, table_name: str, schema_name: str, engine: Engine,
							 if_exists: str = 'replace') -> None:
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
		logging.info(f"Data successfully written to DB: {schema_name}.{table_name}.")
	except Exception as e:
		logging.error(f"Error while writing data to {table_name}: {e}")
		raise


def write_geopandas_to_postgis(
		gdf: gpd.GeoDataFrame,
		table_name: str,
		schema_name: str,
		engine: Engine,
		if_exists: str = "replace",
		sindex: bool = False,
		srid: str = "EPSG:31370",
		geometry_col: str = "geometry"
):
	"""
	Write a geodataframe to a postgres table. If multiple geometry types are found, the write happens in multiple steps.

	:param df: The source geodataframe
	:param table_name: the table name
	:param schema_name: The target schema
	:param engine: the database engine
	:param if_exists: The action to take when the table name already exists (replace, append, fail)
	:param index: Flag indicating the creation of a spatial index on the geom column
	:param srid: A string representation of the SRID (e.g. EPSG:31370). If not given, will be deduced from dataset.
	:param geometry_col: A string representing the geometry column name. default is "geometry"
	:return:
	"""
	logging.info("Start writing to postgis table {}".format(table_name))

	import pyproj
	crs = pyproj.CRS(srid)

	# srid = gdf.crs.to_epsg()
	srid = crs.to_epsg()
	# logging.info(f'crs: {gdf.crs}')
	gtypes = [geom for geom in gdf.geom_type.unique()]
	logging.info(f"The following geometry types were found in {table_name}: {gtypes}")
	gdf["geom"] = gdf[geometry_col].apply(
		lambda x: WKTElement(x.wkt, srid=srid) if x else None
	)  # geom_masker = gdf['geometry']

	# drop the geometry column as it is now duplicative
	# gdf.drop('geometry', 1, inplace=True)
	wantedcols = [col for col in gdf.columns.values if col != geometry_col]

	# Use 'dtype' to specify column's type
	# For the geom column, we will use GeoAlchemy's type 'Geometry'
	# for idx, gtype in enumerate(gtypes):
	# exists = exists if idx == 1 else 'append'
	# mask = geom_masker.geom_type == gtype
	gtype = gtypes[0] if len(gtypes) == 1 else "geometry"
	logging.info(f"Using geom type {gtype} and srid {srid} for {table_name}. Importing {len(gdf)} features")

	gdf[wantedcols].to_sql(
		table_name,
		engine,
		schema=schema_name,
		if_exists=if_exists,
		index=False,
		dtype={"geom": Geometry(gtype, srid=srid)},
		chunksize=10000  # Write in manageable chunks
	)
	if sindex:
		with e.connect() as con:
			con.execute(
				f"CREATE INDEX {table_name}_gix ON {table_name} USING GIST(geom);"
			)
	# gdf.drop("geom") #todo fix this
	logging.info("Finished writing postgis table {}".format(table_name))


def write_geopandas_to_file(gdf: gpd.GeoDataFrame, foldername: str, layername: str, driver: str = "ESRI Shapefile"):
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


def ogr_load_data_to_geopackage(ogr_path, geopackage_path, source_path, source_type, layer_name=None, connection_string=None,
								overwrite=True):
	"""
    Load a spatial dataset into a GeoPackage using ogr2ogr, supporting multiple input types.

    :param ogr_path: Path to the ogr2ogr executable
    :param geopackage_path: Path to the target GeoPackage file
    :param source_path: Path to the input spatial data (e.g., Shapefile, GeoJSON, GeoPackage) or "schema.table" for PostGIS
    :param source_type: Type of input data ("shapefile", "geopackage", "geojson", "postgis")
    :param layer_name: Name of the layer in the GeoPackage (will match table name for PostGIS)
    :param connection_string: PostGIS connection string (required if source_type is "postgis")
    :param overwrite: Whether to overwrite the existing layer in the GeoPackage (default: True)$
    	"""
	__default_schema_if_postgis = 'public'
	features_in_geopackage_path = []
	if not overwrite and os.path.exists(geopackage_path):
		features_in_geopackage_path = geou.list_all_features_in_geopackage_sqlite(geopackage_path)


	# Determine source parameter based on type
	if source_type == "postgis":
		if not connection_string:
			raise ValueError("PostGIS connection string must be provided when source_type is 'postgis'")
		source = connection_string
		schema, table_name = source_path.split(".", 1) if "." in source_path else (__default_schema_if_postgis, source_path)
		layer_name = layer_name or table_name  # Default to table name if not provided
		sql_query = f"SELECT * FROM {schema}.{table_name}"

	elif source_type in ["shapefile", "geopackage", "geojson"]:
		source = source_path
		if not layer_name:
			layer_name = os.path.basename(source).split(".")[0]

	else:
		raise ValueError(f"Unsupported source_type: {source_type}")

	if not overwrite and layer_name in features_in_geopackage_path:
		logging.warning(f"Not loading {layer_name} because it already exists in target geopackage")
		return
	ogr2ogr_command = [
		ogr_path,
		"-f", "GPKG",
		"-update",
		"-overwrite" if overwrite else "-append",
		geopackage_path,
		source,
		"-nln", layer_name,
		"-lco", "SPATIAL_INDEX=YES"
	]

	if source_type == "postgis":
		ogr2ogr_command.extend(["-sql", sql_query])

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
		target_crs=None
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
	:param target_crs: Target CRS (e.g. 'EPSG:4326').
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

	if target_crs:
		command.extend(['-t_srs', target_crs])
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
def pandas_to_geopandas(df, geom_col='geom', crs=packageConfig.DEFAULT_CRS):
	"""
	Convert a DataFrame to a GeoDataFrame.

	This function converts a pandas DataFrame with a column containing geometries (as WKT strings or Shapely geometries)
	into a GeoDataFrame. If the geometries are provided as WKT strings, the function attempts to load them using the
	`wkt.loads` method. It also handles empty or invalid geometries gracefully.

	:param df: The source DataFrame to be converted into a GeoDataFrame.
	:param geom_col: The name of the column containing the geometries. Defaults to 'geom'.
	:param crs: The coordinate reference system to assign to the resulting GeoDataFrame.
				Defaults to 'EPSG:4326' if not provided.
	:return: A GeoDataFrame with the geometry column specified.
	"""
	if geom_col not in df.columns:
		raise ValueError(f"Geometry column '{geom_col}' not found in DataFrame.")

	# Handle geometry conversion (from WKT strings if necessary)

	df[geom_col] = df[geom_col].apply(geou.safe_wkt_load)

	# Create and return the GeoDataFrame
	return gpd.GeoDataFrame(df, geometry=geom_col, crs=crs)


def geopandas_to_polars(geopandas_gdf, geom_col: str = "geometry"):
	"""
	Reads a spatial dataset from a path into a GeoDataFrame, then converts it to a Polars DataFrame.
	The geometry column is converted to WKT format.

	:param geopandas_gdf: The geopandas dataframe
	:param geom_col: The name of the geometry column to be created in the DataFrame (default is "geometry")
	:returns: A Polars DataFrame with properties and geometry in WKT format
	"""

	# Convert the geometry column to WKT
	geopandas_gdf[geom_col] = geopandas_gdf[geom_col].apply(lambda geom: geom.wkt if geom is not None else None)

	# Convert GeoDataFrame to Polars DataFrame
	pl_df = pandas_to_polars(geopandas_gdf)

	return pl_df
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

@time_function
def polars_to_geoparquet(polars_df, geoparquet_path: str, geom_col: str = "geometry", crs: str = packageConfig.DEFAULT_CRS):
	logging.info(f"Start converting polars to geoparquet. n={len(polars_df)} rows. crs = {crs}")
	pdf = polars_to_pandas(polars_df)
	pdf_geo = pandas_to_geopandas(pdf, geom_col, crs)
	pdf_geo.to_parquet(geoparquet_path)
