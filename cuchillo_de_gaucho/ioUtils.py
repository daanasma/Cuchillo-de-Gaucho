from . import winUtils as wu
from . import geoUtils as gu
from . import pathUtils as pu
from . import pgUtils as pgu
import os
import geopandas as gpd

import logging


#READING
def read_to_geodataframe(path: str, driver: str = "ESRI Shapefile") -> gpd.GeoDataFrame:
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


#WRITING

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

	print(command)
	# Run the command as a subprocess
	wu.run_subprocess(command)
