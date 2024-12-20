import pandas as pd
import geopandas as gpd
import logging


def filter_dataframe_is_in(df: pd.DataFrame, attribute: str, range: list):
	subset = df[df[attribute].isin(range)]
	return subset


def spatial_select_geodataframe(gdf: gpd.geodataframe, selection_mask_gdf: gpd.GeoDataFrame,
								predicate="within", crs="EPSG:31370", tolerance_m=0.5):

	# Apply a buffer to the selection geometries
	# Positive buffer for expanding the geometry
	gdf = gdf.to_crs(crs)
	selector = selection_mask_gdf.to_crs(crs)

	selector["geometry"] = selector.geometry.buffer(tolerance_m)
	logging.info(f'Start selection of subset of geodataframe (spatial relationship: {predicate})')
	subset = gpd.sjoin(gdf, selector, predicate=predicate)
	logging.info(f'Success: got subset. size = {len(subset)}')
	return subset
