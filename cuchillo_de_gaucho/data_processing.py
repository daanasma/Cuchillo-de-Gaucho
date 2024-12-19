# Data processing functions for cuchillo-de-gaucho package 
def clean_data(data): 
    """Cleans data by stripping whitespace from each item.""" 
    return [x.strip() for x in data] 
 
def filter_data(data, condition): 
    """Filters data based on the given condition.""" 
    return [item for item in data if condition(item)] 
