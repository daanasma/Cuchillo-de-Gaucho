import pandas as pd
import geopandas as gpd
import polars as pl
import logging

#### PANDAS #####
def filter_pandas_df_with_sql(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
	return df.query(sql_query)
def filter_pandas_df_is_in(df: pd.DataFrame, attribute: str, range: list):
	subset = df[df[attribute].isin(range)]
	return subset

### GEOPANDAS ###
def spatial_select_geodataframe(gdf: gpd.geodataframe, selection_mask_gdf: gpd.GeoDataFrame,
								add_select_attr=False, predicate="within", crs="EPSG:31370", tolerance_m=0.5):

	# Apply a buffer to the selection geometries
	# Positive buffer for expanding the geometry
	gdf = gdf.to_crs(crs)
	selector = selection_mask_gdf.to_crs(crs)

	selector["geometry"] = selector.geometry.buffer(tolerance_m)
	logging.info(f'Start selection of subset of geodataframe (spatial relationship: {predicate})')
	subset = gpd.sjoin(gdf, selector, predicate=predicate)
	if not add_select_attr:
		# Drop columns from the right GeoDataFrame (selector) if you only want left attributes
		columns_to_drop = [col for col in subset.columns if col not in gdf.columns]
		subset = subset.drop(columns=columns_to_drop)

	logging.info(f'Found {len(subset)} records.')

	subset = subset.drop_duplicates()
	logging.info(f'Success: got subset, dropped duplicates. size = {len(subset)}')
	return subset

### POLARS ###
def polars_add_constant_column(df: pl.DataFrame, column_name: str, value) -> pl.DataFrame:
    """
    Adds a column to the Polars DataFrame with a constant value.

    Parameters:
        df (pl.DataFrame): The input Polars DataFrame.
        column_name (str): The name of the new column to be added.
        value: The constant value to fill the new column with.

    Returns:
        pl.DataFrame: A new Polars DataFrame with the added column.
    """
    return df.with_columns(
        pl.lit(value).alias(column_name)
    )
