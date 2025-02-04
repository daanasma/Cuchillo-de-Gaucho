import logging
import sqlite3
from shapely import wkt

def safe_wkt_load(geom):
	"""
	Safely load a WKT (Well-Known Text) string into a Shapely geometry object.

	This function attempts to convert a WKT string into a Shapely geometry object.
	If the geometry string is invalid or empty, it returns None instead of raising an error.

	:param geom: A WKT string or None. If None or an invalid WKT string is passed, the function returns None.
	:return: A Shapely geometry object if the WKT is valid, otherwise None.
	"""
	try:
		return wkt.loads(geom) if geom else None
	except Exception:
		return None


def list_all_features_in_geopackage_sqlite(gpkg_file):
	"""
	List all feature tables in a GeoPackage file.

	This function connects to a GeoPackage SQLite file and retrieves all tables that contain spatial features
	(i.e., tables with data type 'features'). It logs and returns a list of all feature table names.

	:param gpkg_file: The path to the GeoPackage file.
	:return: A list of feature table names within the GeoPackage file.
	"""
	conn = sqlite3.connect(gpkg_file)
	cursor = conn.cursor()
	cursor.execute("SELECT table_name FROM gpkg_contents WHERE data_type = 'features';")
	tables = cursor.fetchall()

	logging.debug("All tables in the GeoPackage:")
	for table in tables:
		logging.debug(table[0])
	conn.close()
	return [table[0] for table in tables]
