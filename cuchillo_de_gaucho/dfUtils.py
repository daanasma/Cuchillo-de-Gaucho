import pandas as pd
import geopandas as gpd
from shapely import wkt
import polars as pl
import logging
import re

from . import geoUtils as geou
from . import helperUtils as hu


#### PANDAS #####
def filter_pandas_df_with_sql(df: pd.DataFrame, sql_query: str) -> pd.DataFrame:
    """
    Filter a pandas DataFrame (or GeoDataFrame) using an SQL-like query.

    This function allows you to filter a DataFrame based on a query string written in SQL-like syntax.
    It uses pandas' built-in `query` method to execute the query and return a subset of the DataFrame
    that matches the conditions.

    :param df: The pandas DataFrame (or GeoDataFrame) to be filtered.
    :param sql_query: The SQL-like query to filter the DataFrame. Should follow pandas query syntax.
    :return: A DataFrame containing only the rows that satisfy the query conditions.

    Logs:
        INFO log will be generated showing the executed query and the number of rows remaining
        after filtering.
    """
    res_df = df.query(sql_query)
    logging.info(f"Filtered dataframe with {sql_query}. Remaining: {len(res_df)} rows.")
    return res_df


def filter_pandas_df_is_in(df: pd.DataFrame, attribute: str, range: list):
    subset = df[df[attribute].isin(range)]
    return subset


def pandas_series_remove_string_occurrences(series: pd.Series, patterns: list) -> pd.Series:
    """
    Removes all occurrences of specified patterns from a given pandas Series.

    Args:
    series (pd.Series): The Series to clean.
    patterns (list): A list of substrings or regex patterns to remove.

    Returns:
    pd.Series: A cleaned pandas Series.
    """
    # Combine all patterns into a single regex pattern
    combined_pattern = '|'.join(map(re.escape, patterns))

    # Apply regex replacement
    return series.str.replace(combined_pattern, '', regex=True)


def pandas_series_keep_only_numbers(series: pd.Series) -> pd.Series:
    """
    Removes all non-numeric characters from a pandas Series.

    Args:
    series (pd.Series): The Series to clean.

    Returns:
    pd.Series: A Series containing only numeric characters.
    """
    return series.replace(r'\D+', '', regex=True)  # \D matches any non-digit


def pandas_clean_dataframe_remove_substrings_from_column(df: pd.DataFrame, src_column: str, target_column: str,
                                                         patterns_to_remove: list) -> pd.DataFrame:
    """
    Clean specified patterns from a source column and store results in a target column.

    Args:
    df (pd.DataFrame): The DataFrame to process.
    src_column (str): Name of the source column to clean.
    target_column (str): Name of the target column to store cleaned results.
    patterns_to_remove (list): List of substrings or patterns to remove.

    Returns:
    pd.DataFrame: Updated DataFrame with cleaned target column.
    """
    df[target_column] = pandas_series_remove_string_occurrences(df[src_column], patterns_to_remove)
    return df


def pandas_clean_dataframe_keep_numbers(df: pd.DataFrame, src_column: str, target_column: str) -> pd.DataFrame:
    """
    Removes all non-numeric characters from the source column and stores the result in the target column.

    Args:
    df (pd.DataFrame): The DataFrame to process.
    src_column (str): Name of the source column to clean.
    target_column (str): Name of the target column to store cleaned results.

    Returns:
    pd.DataFrame: Updated DataFrame with only numeric content in the target column.
    """
    df[target_column] = pandas_series_keep_only_numbers(df[src_column])
    return df


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

def polars_classify_column(df: pl.DataFrame, col_name: str, ranges: dict, new_col_name: str, drop_input_col=True) -> pl.DataFrame:
    """
    Classify values in a Polars DataFrame based on predefined ranges.

    This function assigns category labels to values in a specified column based on a dictionary of range boundaries.
    It dynamically constructs a `when-then-otherwise` expression in Polars to apply the classification.

    :param df: The Polars DataFrame to process.
    :param col_name: The name of the source column containing numeric values.
    :param ranges: A dictionary where keys are category labels and values are (min, max) tuples defining range boundaries.
    :param new_col_name: The name of the target column to store the classified results.
    :param drop_input_col: If true, the input column will be dropped from the result dataframe.
    :return: A Polars DataFrame with a new column containing the classified values.

    """
    def classify_value(value):
        # This function has access to the classification_dict in the namespace
        for label, (lower, upper) in ranges.items():
            if lower <= value < upper:
                return label
        return None  # If no classification fits, return None

    df = df.with_columns((
        pl.col(col_name).map_elements(classify_value).alias(new_col_name)
    ))

    if drop_input_col:
        df = df.drop(col_name)

    return df

def polars_clean_dataframe_replace_substrings(df: pl.DataFrame, src_column: str, target_column: str,
                                              patterns_dict: dict[str, str]) -> pl.DataFrame:
    """
    Clean specified patterns from a source column based on a dictionary of replacements
    and store results in a target column.

    Args:
    df (pl.DataFrame): The DataFrame to process.
    src_column (str): Name of the source column to clean.
    target_column (str): Name of the target column to store cleaned results.
    patterns_dict (dict[str, str]): A dictionary where keys are substrings/patterns to find,
                                    and values are the substrings to replace them with.
                                    If the value is an empty string, the pattern will de facto be removed.

    Returns:
    pl.DataFrame: Updated DataFrame with cleaned target column.
    """
    # Start with the source column expression
    clean_expr = pl.col(src_column)
    for pattern, replacement in patterns_dict.items():
        escaped_pattern = re.escape(pattern)
        clean_expr = clean_expr.str.replace_all(escaped_pattern, replacement)

    return df.with_columns(
        clean_expr.alias(target_column)
    )


def polars_clean_dataframe_keep_numerical_substrings(df: pl.DataFrame, src_column: str,
                                                     target_column: str) -> pl.DataFrame:
    """
    Keep only the numerical substrings from a source column and store the result in a target column.

    Args:
    df (pl.DataFrame): The DataFrame to process.
    src_column (str): Name of the source column to extract numerical substrings from.
    target_column (str): Name of the target column to store results.

    Returns:
    pl.DataFrame: Updated DataFrame with the target column containing only numerical substrings.
    """
    # Extract all digits from the source column and join them together
    clean_expr = (
        pl.col(src_column)
        .str.extract_all(r'[-+]?(?:\d*\.*\d+)')
        .list.join(" ")  # Join all matches with a space separator
        .fill_null("")  # Replace nulls with empty string
        .alias(target_column)
    )
    print(clean_expr)

    return df.with_columns(clean_expr.alias(target_column))
