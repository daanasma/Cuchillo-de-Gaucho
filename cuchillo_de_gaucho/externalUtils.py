import requests
from . import winUtils as wu
from . import packageConfig

#todo fix  :)
class Geocoder():
	def __init__(self, geocoding_engines, cache_dir, crs: str = 'EPSG:31370', silent=True):
		self.geocoding_engines = self.set_geocoding_engines(geocoding_engines)
		self.cache_dir = cache_dir
		self.crs = crs
		self.silent = silent
		wu.create_folder_if_not_exists(cache_dir)

	def set_geocoding_engines(self, geocoding_engines):
		geocoding_engines = {}
		for engine in geocoding_engines:
			endpoint = config.EXTERNAL_ENDPOINTS.get('geocoding').get(engine)
			if endpoint:
				geocoding_engines[engine] = {'endpoint': endpoint}

		#todo handle errors
		return geocoding_engines


	# def geocode(self, address: dict, place_name: str = None):
	# 	"""
	# 	Geocode an address to XY coordinates in Lambert72 or WGS84 coordinate reference system (CRS).
	#
	# 	Parameters:
	# 		address (dict): The address to be geocoded, with optional components such as 'straatnaam', 'huisnummer',
	# 						'postcode', 'gemeentenaam', and 'address_string'.
	# 		place_name (str, optional): An additional place name to refine the geocoding query.
	# 									Default is None.
	# 		crs (str, optional): The coordinate reference system to use for output.
	# 							 Default is 'EPSG:31370' (Belgian Lambert 1972).
	# 							 Supported values: 'EPSG:31370' for Lambert72, 'WGS84' for WGS84.
	# 		cache_dir (str, optional): Directory path to cache geocoding results.
	# 								   Default is config.DATA_DIR_CACHE (defined in config file).
	# 		silent (bool, optional): If True, suppress logging of information and warnings.
	# 								 Default is True.
	# 		response_format (str, optional): Format of the returned geocoding result.
	# 										 'coordstr' returns a coordinate string.
	# 										 'response_dict' returns a dictionary with detailed response.
	# 										 Default is 'coordstr'.
	# 		geocoding_engines (list, optional): List of geocoding engines to use in order of preference.
	# 											Default is None, which uses ['aiv_adresmatch', 'tomtom'].
	#
	# 	Returns:
	# 		str or dict: A string containing the XY coordinates of the address in the specified CRS if response_format is 'coordstr'.
	# 					 For Lambert72 (EPSG:31370), the format is "<X> <Y>".
	# 					 For WGS84, the format is "<Longitude> <Latitude>".
	# 					 If response_format is 'response_dict', returns a dictionary with detailed response including source and score.
	# 					 Returns None if geocoding fails or the result is empty.
	# 	"""
	# 	sources = ['aiv_adresmatch', 'tomtom']
	#
	#
	# 	for source in sources:
	# 		r = None
	# 		g = None
	#
	# 		ac = address.get('components')
	# 		query = address.get('address_string')
	# 		if query:
	# 			if source == 'tomtom':
	# 				if place_name:
	# 					query = f'{place_name}, {query}'
	# 		else:
	# 			if ac:
	# 				query = f"{ac.get('straatnaam')} {ac.get('huisnummer')}, {ac.get('postcode')} {ac.get('gemeentenaam')}"
	#
	# 		query = query.replace(' None', '')
	# 		query = query.replace(' nan', '')
	# 		query = query.replace('  ', ' ')
	#
	# 		query = query.lower()
	# 		# open cache if exitsts
	# 		if cache_dir:
	# 			cached_result = os.path.join(cache_dir, source, f'{query}.json')
	# 			if os.path.exists(cached_result):
	# 				r = read_response_from_json(cached_result)
	# 				if not silent:
	# 					logging.info(f'Found cache result file: {cached_result}')
	# 		# if there is no cache.
	#
	# 		if not r:
	# 			if source == 'aiv_geoloc':
	# 				logging.info('GEOCODE AIV')
	# 				r = geocode_address_aiv(query)
	# 				if not silent:
	# 					logging.info(f'geocoded AIV (aiv_geoloc): {r}')
	#
	# 			elif source == 'aiv_adresmatch':
	# 				r = geocode_adresmatch_aiv(ac)
	# 				if not silent:
	# 					logging.info(f'geocoded aiv - match: {r}')
	#
	# 			elif source == 'tomtom':
	# 				r = geocode_fuzzy_tomtom(query)
	# 				if not silent:
	# 					logging.info(f'geocoded Tomtom: {r}')
	#
	# 			if cache_dir:
	# 				write_cache_to_json(cached_result, r)
	# 		if r:
	# 			if source == 'aiv_geoloc':
	# 				g = aiv_gc_response_to_xy(r)
	# 			elif source == 'aiv_adresmatch':
	# 				g = aiv_am_response_to_xy(r)
	# 			elif source == 'tomtom':
	# 				g = tomtom_response_to_xy(r)
	#
	# 		if g:
	# 			if response_format == 'coordstr':
	# 				if crs == 'EPSG:31370':
	# 					x = g.get('X_Lambert72')
	# 					y = g.get('Y_Lambert72')
	# 					return f"{x} {y}"
	#
	# 				elif crs == 'WGS84':
	# 					lat = g.get('Lat_WGS84')
	# 					lng = g.get('Lon_WGS84')
	# 					return f"{lng} {lat}"
	# 			elif response_format == 'response_dict':
	# 				g['source'] = source
	# 				if source == 'aiv_geoloc':
	# 					g['score'] = 100
	# 				return g
	# 		else:
	# 			logging.info(f"{source} - {address} -- Failed to find a coordinate")
	# 	logging.warning(f"{query} -- Failed to find a coordinate")
	# 	return None
