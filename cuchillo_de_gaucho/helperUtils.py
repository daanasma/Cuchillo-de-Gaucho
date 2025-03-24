
def generate_zero_based_index(num, length=3):
    """
    Generates a zero-padded index based on the input number, with a fixed length.

    Args:
        num (int): The number to convert into a zero-padded string.
        length (int): The fixed length of the returned string (default is 3).

    Returns:
        str: A zero-padded index with the specified length.
    """
    return str(num).zfill(length)

