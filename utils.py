import hashlib
import pandas as pd
from datetime import datetime


def get_lookup_fn(df, from_col, to_col):
    """
    From a pd.DataFrame, created a lookup dictionary that makes it easy to find a value in the `to_col`
    given a value in the `from_col`. It can be useful for example to look up unit price based on product ID.

    Params
    ------
    df : pd.DataFrame
        Source DataFrame to generate the lookup dict from.
    from_col : str
        Column name from which the values should become the keys in the lookup dict.
    to_col : str
        Column name from which the values should become the values in the lookup dict.

    Returns
    -------
    lookup_fn : Function
        A function that can be called with a value from the `from_col` and returns the corresponding value from the `to_col`.

    Raises
    ------
    AssertionError
        If one of the passed columns are not in the DataFrame.
    """

    assert from_col in df.columns, "`from_col` is not a valid column name in the passed DataFrame."
    assert to_col in df.columns, "`to_col` is not a valid column name in the passed DataFrame."

    lookup_dict = pd.Series(df[to_col].values, index=df[from_col]).to_dict()

    return lambda from_val: lookup_dict[from_val]


def hash_id(id_str):
    """
    Takes a string with arbitrary length and hashes it to a 16 character long alphanumeric sequence.

    Params
    ------
    id_str : str
        An input string with arbitrary length. It should uniquely identify the entity that it's associated with.

    Returns
    -------
    hashed_id : str
        A 16 character long alphanumeric string.
    """

    return hashlib.shake_256(str(id_str).encode("utf-8")).hexdigest(16)


def parse_date_formats(date, formats):
    """
    Attempts to parse a date passed as a string given a list of possible formats. If none of the formats
    are matching, it raises an exception.

    Params
    ------
    date : str
        Date to be parsed as a string.

    formats : str[]
        List of possible date formats.

    Returns
    -------
    date : datetime
        Parsed date as a datetime object.

    Raises
    ------
    ValueError
        If the date cannot be parsed using any of the passed formats.
    """

    i = 0
    while i < len(formats):
        try:
            return datetime.strptime(date, formats[i])
        except ValueError:
            i += 1

    raise ValueError(f"Could not parse date: {date}")
