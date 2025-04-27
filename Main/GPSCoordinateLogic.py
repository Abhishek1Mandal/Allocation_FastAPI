import pandas as pd
import requests
from typing import List, Tuple, Optional
from config.GpsConfig import API_KEYS  # Import from config


def get_lat_lon(address: str, api_key: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Fetch GPS coordinates for a given address using the HERE Geocoding API.

    Args:
        address (str): The address to geocode.
        api_key (str): The API key for HERE Geocoding API.

    Returns:
        Tuple[Optional[float], Optional[float]]: Latitude and longitude as floats, "noGPS" for no results, or None for errors.
    """
    base_url = "https://geocode.search.hereapi.com/v1/geocode"
    params = {"q": address, "apikey": api_key}

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()
        if data["items"]:
            position = data["items"][0]["position"]
            return float(position["lat"]), float(position["lng"])
        else:
            print(f"No GPS coordinates found for address: {address}")
            return "noGPS", "noGPS"

    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:
            print("Error: Rate limit exceeded. HTTP Status code: 429")
            return None, None
        else:
            print(
                f"Error: Failed to retrieve data. HTTP Status code: {response.status_code}"
            )
            return None, None
    except Exception as e:
        print(f"Error: Failed to fetch coordinates for {address}. Error: {str(e)}")
        return None, None


def process_dataframe(file_path: str, api_keys: List[str] = API_KEYS) -> str:
    """
    Process an Excel file to add GPS coordinates to the 'Cus_Add' column.

    Args:
        file_path (str): Path to the input Excel file.
        api_keys (List[str]): List of API keys for HERE Geocoding API. Defaults to API_KEYS from config.

    Returns:
        str: Path to the output Excel file with GPS coordinates.

    Raises:
        ValueError: If 'Cus_Add' column is not found in the Excel file or no API keys are available.
    """
    if not api_keys:
        raise ValueError("No API keys provided or loaded from .env file.")

    df = pd.read_excel(file_path)

    if "Cus_Add" not in df.columns:
        raise ValueError("Column 'Cus_Add' not found in the Excel file.")

    if "latitude" not in df.columns:
        print("latitude column not found, creating...")
        df["latitude"] = pd.NA
    if "longitude" not in df.columns:
        print("longitude column not found, creating...")
        df["longitude"] = pd.NA

    exhausted_keys = set()

    for index, row in df.iterrows():
        if index > 0 and index % 100 == 0:
            print(f"Processed {index} rows so far.")

        if pd.notna(row["latitude"]) and pd.notna(row["longitude"]):
            continue

        api_key_exhausted = True
        for current_api_key in [key for key in api_keys if key not in exhausted_keys]:
            lat, lng = get_lat_lon(row["Cus_Add"], current_api_key)

            if lat is None and lng is None:
                print(
                    f"Rate limit or error encountered with API key: {current_api_key}. Trying next API key..."
                )
                exhausted_keys.add(current_api_key)
                continue

            df.at[index, "latitude"] = lat
            df.at[index, "longitude"] = lng
            api_key_exhausted = False
            break

        if api_key_exhausted:
            print(
                f"Error: All API keys exhausted at row {index}. Stopping the process."
            )
            break

    # Ensure latitude and longitude are float type, with "noGPS" preserved
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").fillna("noGPS")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").fillna("noGPS")

    output_path = file_path.replace(".xlsx", "_with_GPS.xlsx")
    df.to_excel(output_path, index=False)
    return output_path