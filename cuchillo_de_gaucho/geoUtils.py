import logging
import sqlite3


def list_all_features_in_geopackage_sqlite(gpkg_file):
	"""

	:param gpkg_file:
	:return:
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
