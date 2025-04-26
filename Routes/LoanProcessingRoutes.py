from fastapi import APIRouter, UploadFile, File, Form
from Main.LoanProcessingService import process_loans

router = APIRouter()


@router.post("/process-loans")
async def process_loans_route(
    password: str = Form(...),
    loan_numbers_column: str = Form(...),
    file: UploadFile = File(...),
):

    # Check if the uploaded file is an Excel file
    if not file.filename.endswith(".xlsx"):
        return {"error": "Only .xlsx files are supported"}

    # Process the loan numbers
    result = process_loans(password, loan_numbers_column, file.file)
    return result
