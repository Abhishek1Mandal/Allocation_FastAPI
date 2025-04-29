import pandas as pd
import pymongo
from fastapi import HTTPException
import os
import pytz
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the expected columns (exact names to be preserved)
EXPECTED_COLUMNS = [
    "Assigned_FOS_ID",
    "Assigned_FOS",
    "LoanNo/CC",
    "Cus_Name",
    "Cus_Mobile",
    "Port",
    "POS",
    "TAD",
    "EMI",
    "BKT/DPD",
    "Cus_Add",
    "Perma_Add",
    "Emp_Address",
    "TC_ID",
    "TC_Name",
    "TL_ID",
    "TL_Name",
    "Asset/Product",
    "Masked_LoanNo/CC",
    "acceptanceStatus",
    "assignedStatus",
]

def insert_data_from_excel(password: str, file) -> dict:
    # Validate password
    valid_password = os.getenv("VALID_PASSWORD")
    if password != valid_password:
        raise HTTPException(status_code=401, detail="Invalid password")

    # Load environment variables
    mongo_uri = os.getenv("ATLAS_MONGO_URI")
    database_name = os.getenv("MONGO_DATABASE")
    collection_name = os.getenv("MONGO_COLLECTION")

    if not all([mongo_uri, database_name, collection_name]):
        raise HTTPException(
            status_code=500,
            detail="Missing required environment variables (ATLAS_MONGO_URI, MONGO_DATABASE, or MONGO_COLLECTION)",
        )

    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Read the Excel file
        try:
            df = pd.read_excel(file)
            if df.empty:
                client.close()
                return {
                    "message": "The uploaded Excel file is empty.",
                    "inserted_documents": 0,
                }

            # Strip whitespace from column names but preserve case
            df.columns = df.columns.str.strip()

            # Validate columns (case-sensitive)
            missing_columns = [col for col in EXPECTED_COLUMNS if col not in df.columns]
            if missing_columns:
                client.close()
                raise HTTPException(
                    status_code=400,
                    detail=f"Excel file is missing required columns: {', '.join(missing_columns)}"
                )

            # Generate timestamp in Indian Standard Time (IST)
            indian_timezone = pytz.timezone("Asia/Kolkata")
            indian_timestamp = datetime.now(indian_timezone)

            # Add required fields if not present, using exact names
            df["assignedTimestamp"] = indian_timestamp
            if "acceptanceStatus" not in df.columns:
                df["acceptanceStatus"] = "pending"  # Set default value
            if "assignedStatus" not in df.columns:
                df["assignedStatus"] = "unassigned"  # Set default value
            if "Distance(KM)" not in df.columns:
                df["Distance(KM)"] = 0.0  # Set default value

            # Convert DataFrame to dictionary records
            records = df.to_dict(orient="records")

            # Insert data into the collection
            result = collection.insert_many(records)
            inserted_count = len(result.inserted_ids)

            logger.info(f"Successfully inserted {inserted_count} documents into {collection_name}")

            client.close()

            return {
                "message": f"Data uploaded successfully to {collection_name} in {database_name} database.",
                "inserted_documents": inserted_count,
            }

        except Exception as e:
            client.close()
            raise HTTPException(
                status_code=400, detail=f"Failed to read Excel file: {str(e)}"
            )

    except Exception as e:
        client.close()
        raise HTTPException(
            status_code=500,
            detail=f"Failed to connect to MongoDB or insert data: {str(e)}",
        )