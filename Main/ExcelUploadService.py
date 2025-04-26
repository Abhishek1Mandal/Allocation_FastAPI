import pandas as pd
import pymongo
from fastapi import HTTPException
import os
import pytz
from datetime import datetime


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

            # Generate timestamp in Indian Standard Time (IST)
            indian_timezone = pytz.timezone("Asia/Kolkata")
            indian_timestamp = datetime.now(indian_timezone)

            # Add a new column 'assignedTimestamp' with the IST timestamp
            df["assignedTimestamp"] = indian_timestamp
            # df['acceptanceStatus'] = 'pending'  # Commented out as in original

            # Convert DataFrame to dictionary records
            records = df.to_dict(orient="records")

            # Insert data into the collection
            result = collection.insert_many(records)
            inserted_count = len(result.inserted_ids)

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
