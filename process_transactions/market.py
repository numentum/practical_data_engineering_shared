import json
import requests
import pandas as pd
from statistics import mean
from datetime import datetime
from geopy.geocoders import Nominatim
from difflib import get_close_matches

from practical_data_engineering_shared.io.extract import extract_table
from practical_data_engineering_shared.io.load import load_dataframe
from practical_data_engineering_shared.io.drive import parallel_load_files_to_df
from practical_data_engineering_shared.utils import get_lookup_fn, hash_id, parse_date_formats
from practical_data_engineering_shared.constants import LOCATIONS, EMPLOYEES, TAX_RATE, PRODUCTS


def get_coordinates(address):
    """
    Translates an address to coordinates.

    Params
    ------
    address : str
        Address or city name.

    Returns
    -------
    latitude : float
        Latitude part of the coordinate.

    longitude : float
        Longitude part of the coordinate.
    """

    locator = Nominatim(user_agent="GetLoc")
    location = locator.geocode(address)

    return location.latitude, location.longitude


def get_weather_type(weather_data):
    """
    Categorizes weather from cloud cover, rain amount and snow amount.

    Params
    ------
    weather_data : dict
        Full data JSON returned by the Open Meteo weather API when the following params are passed:
        daily=temperature_2m_max,rain_sum,snowfall_sum,precipitation_hours&hourly=cloudcover

    Returns
    -------
    weather_type : {'sunny', 'rainy', 'snowy', 'cloudy'}
        Categorical weather type inferred from the data.
    """

    cloud_cover = mean(weather_data["hourly"]["cloudcover"][7:15])
    rain = weather_data["daily"]["rain_sum"][0]
    snow = weather_data["daily"]["snowfall_sum"][0]

    if cloud_cover < 10:
        return "sunny"
    elif rain > 2:
        return "rainy"
    elif snow > 0.5:
        return "snowy"
    else:
        return "cloudy"


def get_weather(address, date):
    """
    Gets the max daily temperature and weather type of the passed location at the passed data.

    Params
    ------
    address : str
        Location to get the weather for.
    date : str
        Date to get the weather for. Format should be %Y-%m-%d

    Returns
    -------
    temp : float
        Max daily temperature at the given location.

    weather_type : {'sunny', 'rainy', 'snowy', 'cloudy'}
        Categorical weather type inferred from the data.
    """

    latitude, longitude = get_coordinates(address)

    url = f"https://archive-api.open-meteo.com/v1/era5?latitude={latitude}&longitude={longitude}&start_date={date}&end_date={date}&timezone=GMT&daily=temperature_2m_max,rain_sum,snowfall_sum,precipitation_hours&hourly=cloudcover"
    response = requests.get(url)
    weather_data = json.loads(response.content.decode("utf-8"))

    temp = weather_data["daily"]["temperature_2m_max"][0]
    weather_type = get_weather_type(weather_data)

    return temp, weather_type


def attempt_to_fix_categorical(row, key, allowed_values):
    """
    This function is intended to fix typos in a string if we know the values the string should take.

    Takes a string value and a list of allowed values. If the string is in the list, it returns that string
    as it is. If it's not, it checks if there are any similar strings in the list. If there is at least one
    with a similarity score above the cutoff (0.8), it returns the match. If there is no match, returns the original
    value alongside an error message.

    The reason that the whole row + the key is passed and not just the value is to be able to provide less generic
    and more useful error message.

    Params
    ------
    row : pd.Series
        Pandas row to retrieve the value from.
    key : str
        Column name in the row to retrieve the value with.
    allowed_values : str[]
        List of strings that are allowed.

    Returns
    -------
    value : str
        String with fixed typos (if possible), otherwise the original string untouched.

    error : None | str
        If no match was found, a message describing the error.

    """

    value = row[key]
    if pd.isna(value) or value == "":
        return value, f"Categorical value '{key}' is missing"

    if value in allowed_values:
        return value, None
    else:
        matches = get_close_matches(value, allowed_values, cutoff=0.8)
        if len(matches) > 0:
            return matches[0], None
        else:
            return value, f"Could not correct categorical value '{key}': {value}"


def attempt_to_fix_date_format(date, formats=["%Y-%m-%d", "%y-%m-%d", "%y %m %d"]):
    """
    Takes a date in string format and first checks if its not an empty string, then it tries to
    parse it against multiple formats. If the date could not be parsed, returns an error.

    Params
    ------
    date : str
        Date in string format.

    formats : str[]
        List of format strings to be passed to datetime.strptime()

    Returns
    -------
    date : str
        Date in '%Y-%m-%d' format is parsing was successful, otherwise original date.

    error : None | str
        If the parsing failed, a message describing the error.
    """

    if pd.isna(date) or date == "":
        return date, "Date is missing"
    try:
        parsed_date = parse_date_formats(date, formats=formats)
        return parsed_date.strftime("%Y-%m-%d"), None
    except ValueError:
        return date, f"Could not parse date: {date}"


def validate_time(time_):
    """
    Takes a time in string format and first checks if its not an empty string, then it tries to
    parse it against the '%H:%M' format. If the time could not be parsed, returns an error.

    Params
    ------
    time_ : str
        Time in string format.

    Returns
    -------
    time_ : str
        Time in ''%H:%M' format is parsing was successful, otherwise original date.

    error : None | str
        If the parsing failed, a message describing the error.
    """

    if pd.isna(time_):
        return time_, "Time is missing"
    try:
        datetime.strptime(time_, "%H:%M")
        return time_, None
    except Exception:
        return time_, f"Could not parse time: {time_}"


def add_error(errors, orig_filename, sale_number, error):
    """
    Adds an error to the error registry. The error registry is a dictionary of lists, where
    the keys are the filenames, and in the lists, there are error strings, where the part before : is the sale number
    and the part after the : is the actual error message.

    errors : dict
        Error registry passed by reference.

    orig_filename : str
        Filename of the original (possibly with typos) Excel file.

    sale_number : int
        Sale number or row number that identifies the row where the error is coming from within a file.

    error : str
        Error message describing the situation (e.g. a typo could not be fixed)
    """

    error_string = f"{sale_number}: {error}"
    if orig_filename not in errors:
        errors[orig_filename] = [error_string]
    else:
        errors[orig_filename].append(error_string)


def transform_market_transactions(df, verbose=False):
    # Gather prerequisites of the operations
    products_df = extract_table("products")
    name_to_sku = get_lookup_fn(products_df, from_col="name", to_col="sku")

    transactions = []
    errors = {}
    weather_data = {}
    for i, row in df.iterrows():
        # 7. Try to fix typos in location, employee and product
        location, location_error = attempt_to_fix_categorical(row, "location", LOCATIONS)
        employee, employee_error = attempt_to_fix_categorical(row["additional_data"] if "additional_data" in row else row, "employee", EMPLOYEES)
        product, product_error = attempt_to_fix_categorical(row, "product", list(PRODUCTS.values()))

        # 3. Consolidate different date formats to a common formats
        date, date_error = attempt_to_fix_date_format(row["date"])

        # 4. Validate time
        time_, time_error = validate_time(row["sold_at"])

        # Aggregate errors if needed
        orig_filename = f'{row["location"]}__{row["date"]}__{row["employee"]}'
        error_found = False
        for err in (location_error, employee_error, product_error, date_error, time_error):
            if err is None:
                continue
            add_error(errors, orig_filename, row["sale_number"], err)
            error_found = True

        # Skip to next record if any error was found
        if error_found:
            continue

        # 1. Create unique transaction_id from multiple columns
        fixed_row_id = "market" + location + employee + row["date"] + str(row["sale_number"])

        # Get weather data
        weather_key = date + ":" + location
        if weather_key in weather_data:
            if verbose:
                print(f"Using cached weather data for {date}")
            temp, weather_type = weather_data[weather_key]
        else:
            if verbose:
                print(f"Getting weather data for {date}")
            temp, weather_type = get_weather(location, date)
            weather_data[weather_key] = [temp, weather_type]

        transaction = {
            # 1. Create unique transaction_id from multiple columns
            "transaction_id": hash_id(fixed_row_id),
            # 2. Add location, date and employee from filename
            "location": f"market_{location}",
            # 5. Concatenate date and time to created_at
            "created_at": datetime.strptime(f"{date} {time_}", "%Y-%m-%d %H:%M"),
            # 8. Enrich data with sku from product
            "sku": name_to_sku(product),
            # 9. Add source with constant value 'market'
            "source": "market",
            # 10. Add payment_method with constant value 'cash'
            "payment_method": "cash",
            # 6. Calculate tax and total
            "tax": round(row["unit_price"] * row["quantity"] * TAX_RATE, 2),
            "total": round(row["unit_price"] * row["quantity"] * (1 + TAX_RATE), 2),
            # Add product after attempting to fix typos
            "product_name": product,
            # Move over unit price and quantity unchanged
            "unit_price": row["unit_price"],
            "quantity": row["quantity"],
            "additional_data": {"employee": employee, "temperature": temp, "weather_type": weather_type},
        }

        transactions.append(transaction)

    transactions_df = pd.DataFrame(transactions)

    return transactions_df, errors


def process_market_transactions(verbose=False):
    df = parallel_load_files_to_df(verbose=verbose)
    df, errors = transform_market_transactions(df, verbose=verbose)
    load_dataframe(df)

    return errors
