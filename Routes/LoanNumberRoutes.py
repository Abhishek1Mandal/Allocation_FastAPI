from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse
import os
import uuid
import logging
from Main.LoanNumberProcessor import process_dataframe

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/process_excel/")
async def process_excel(
    file: UploadFile = File(...),
    column_name: str = Form(default="LoanNo/CC"),
    desired_columns: str = Form(default=None),
):
    """
    Process an uploaded Excel file to mask loan numbers, add assignedStatus, and filter columns.
    Defaults to 'LoanNo/CC' column.
    """
    # Validate file type
    if not file.filename.endswith(".xlsx"):
        logger.error(f"Invalid file type: {file.filename}")
        raise HTTPException(status_code=400, detail="Only .xlsx files are supported")

    # Create a temporary file path
    temp_input_path = f"temp/{uuid.uuid4()}_{file.filename}"
    os.makedirs("temp", exist_ok=True)

    try:
        # Save the uploaded file
        logger.info(f"Saving uploaded file to: {temp_input_path}")
        with open(temp_input_path, "wb") as f:
            f.write(await file.read())

        # Process the DataFrame
        logger.info(
            f"Processing file with column: {column_name}, desired_columns: {desired_columns}"
        )
        output_path = process_dataframe(temp_input_path, column_name, desired_columns)

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
        # Clean up the temporary input file
        if os.path.exists(temp_input_path):
            try:
                os.remove(temp_input_path)
                logger.info(f"Cleaned up temporary file: {temp_input_path}")
            except Exception as e:
                logger.error(f"Failed to clean up temporary file: {str(e)}")