import pandas as pd
import pymongo
from fastapi import HTTPException
import os


def process_loans(password: str, loan_numbers_column: str, file) -> dict:

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

    # Read the Excel file
    try:
        df = pd.read_excel(file)
        if loan_numbers_column not in df.columns:
            raise HTTPException(
                status_code=400,
                detail=f"Column '{loan_numbers_column}' not found in the file!",
            )

        # Get unique loan numbers from the column
        unique_loan_numbers = df[loan_numbers_column].dropna().unique().tolist()
        if not unique_loan_numbers:
            raise HTTPException(
                status_code=400, detail="No loan numbers found in the selected column!"
            )
    except Exception as e:
        raise HTTPException(
            status_code=400, detail=f"Failed to read Excel file: {str(e)}"
        )

    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(mongo_uri)
        db = client[database_name]
        collection = db[collection_name]

        # Check for matching loan numbers
        matching_count = collection.count_documents(
            {"LoanNo/CC": {"$in": unique_loan_numbers}}
        )
        if matching_count == 0:
            client.close()
            return {
                "message": "No matching cases found for the given loan numbers.",
                "processed_loan_numbers": len(unique_loan_numbers),
                "matching_cases": 0,
                "updated_documents": 0,
            }

        # Update caseStatus and acceptanceStatus for matching loan numbers
        update_result = collection.update_many(
            {"LoanNo/CC": {"$in": unique_loan_numbers}},
            {"$set": {"caseStatus": "CLOSE_O", "acceptanceStatus": "Resolved"}},
        )

        client.close()

        return {
            "message": "Loan processing completed successfully.",
            "processed_loan_numbers": len(unique_loan_numbers),
            "matching_cases": matching_count,
            "updated_documents": update_result.modified_count,
        }

    except Exception as e:
        client.close()
        raise HTTPException(
            status_code=500, detail=f"Failed to update MongoDB: {str(e)}"
        )
