from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import pandas as pd
import os
from Main.GPSCoordinateLogic import process_dataframe
import shutil
import uuid
from config.GpsConfig import API_KEYS  # Import from config

router = APIRouter()


@router.post("/upload-and-process")
async def upload_and_process_file(file: UploadFile = File(...)):
    """
    Upload an Excel file and process it to add GPS coordinates for addresses in the 'Cus_Add' column.

    Args:
        file (UploadFile): The Excel file to process.

    Returns:
        FileResponse: The processed Excel file with GPS coordinates.
    """
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported.")

    # Validate API keys
    if not API_KEYS:
        raise HTTPException(status_code=500, detail="No API keys configured.")

    # Save the uploaded file temporarily
    temp_file_path = f"temp_{uuid.uuid4()}.xlsx"
    try:
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Validate 'Cus_Add' column
        df = pd.read_excel(temp_file_path)
        if "Cus_Add" not in df.columns:
            raise HTTPException(
                status_code=400, detail="Column 'Cus_Add' not found in the Excel file."
            )

        # Process the file to add GPS coordinates
        output_path = process_dataframe(temp_file_path, API_KEYS)

        # Return the processed file
        return FileResponse(
            path=output_path,
            filename=os.path.basename(output_path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

    finally:
        # Clean up temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)