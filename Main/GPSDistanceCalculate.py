import pandas as pd
import math
from fastapi import HTTPException


# Haversine formula to calculate distance between two points on Earth
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def calculate_distances(file) -> pd.DataFrame:
    try:
        # Read the Excel file
        df = pd.read_excel(file)

        # Expected column names
        required_columns = ["latitude", "longitude", "Fos_latitude", "Fos_longitude"]

        # Check if all required columns are present
        if not all(col in df.columns for col in required_columns):
            missing_cols = [col for col in required_columns if col not in df.columns]
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing_cols)}",
            )

        # Ensure the columns contain numeric values
        for col in required_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Drop rows with invalid or missing data
        df.dropna(subset=required_columns, inplace=True)

        # Calculate the distance and add a new column
        df["Distance(KM)"] = df.apply(
            lambda row: round(
                haversine(
                    row["latitude"],
                    row["longitude"],
                    row["Fos_latitude"],
                    row["Fos_longitude"],
                ),
                3,
            ),  # Round to 3 decimal places
            axis=1,
        )

        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process file: {str(e)}")
