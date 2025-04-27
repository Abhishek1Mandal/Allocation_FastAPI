from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse
import pandas as pd
from Main.LoanNumberMaskForManual import process_dataframe
import os
import uuid
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()

# API endpoint to process Excel file
@router.post("/manual_process_excel/")
async def process_excel(file: UploadFile = File(...), column_name: str = Form(...)):
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        logger.error(f"Invalid file type: {file.filename}")
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Save uploaded file temporarily
    temp_input_path = f"temp/{uuid.uuid4()}_{file.filename}"
    os.makedirs("temp", exist_ok=True)

    try:
        logger.info(f"Saving uploaded file to: {temp_input_path}")
        with open(temp_input_path, "wb") as f:
            f.write(await file.read())

        # Read the Excel file to check columns
        logger.info(f"Checking columns in file: {temp_input_path}")
        df = pd.read_excel(temp_input_path)

        # Process the file with the specified column and required columns
        logger.info(f"Processing file with column: {column_name}")
        output_path = process_dataframe(temp_input_path, column_name)

        # Return the processed file
        logger.info(f"Returning processed file: {output_path}")
        return FileResponse(
            path=output_path,
            filename=os.path.basename(output_path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except ValueError as ve:
        logger.error(f"ValueError during processing: {str(ve)}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected error during processing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")
    finally:
        # Clean up temporary input file
        if os.path.exists(temp_input_path):
            try:
                os.remove(temp_input_path)
                logger.info(f"Cleaned up temporary file: {temp_input_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {str(e)}")
