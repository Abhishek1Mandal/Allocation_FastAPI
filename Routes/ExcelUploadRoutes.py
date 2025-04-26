from fastapi import APIRouter, UploadFile, File, Form
from Main.ExcelUploadService import insert_data_from_excel

router = APIRouter()


@router.post("/upload-excel")
async def upload_excel(password: str = Form(...), file: UploadFile = File(...)):

    # Check if the uploaded file is an Excel file
    if not file.filename.endswith(".xlsx"):
        return {"error": "Only .xlsx files are supported"}

    # Process the Excel file and insert data
    result = insert_data_from_excel(password, file.file)
    return result
