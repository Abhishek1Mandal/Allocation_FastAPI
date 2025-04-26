import os
import shutil
from fastapi import APIRouter, UploadFile, File, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from typing import Optional, List
from pydantic import BaseModel
from Main.AllocationDashboard import (
    secure_filename,
    allowed_file,
    UPLOAD_FOLDER,
    EMPLOYEES_FOLDER,
    CASES_FOLDER,
    get_case_files,
    get_employee_files,
    get_file_columns,
    get_column_values,
    process_files,
    upload_to_db,
)

router = APIRouter()


# Pydantic model for column values request
class ColumnValuesRequest(BaseModel):
    file_name: str
    column_name: str
    file_type: str


# Pydantic model for upload to DB request
class UploadToDBRequest(BaseModel):
    data: List[dict]
    password: str


# Route to upload files
@router.post("/upload")
async def upload_files(
    fos_data: Optional[UploadFile] = File(None),
    master_data: Optional[UploadFile] = File(None),
):
    if fos_data and allowed_file(fos_data.filename):
        fos_filename = secure_filename(fos_data.filename)
        file_path = os.path.join(EMPLOYEES_FOLDER, fos_filename)
        with open(file_path, "wb") as f:
            shutil.copyfileobj(fos_data.file, f)
        return {"message": "File Uploaded Successfully."}
    elif master_data and allowed_file(master_data.filename):
        master_filename = secure_filename(master_data.filename)
        file_path = os.path.join(CASES_FOLDER, master_filename)
        with open(file_path, "wb") as f:
            shutil.copyfileobj(master_data.file, f)
        return {"message": "File Uploaded Successfully."}
    else:
        raise HTTPException(
            status_code=400, detail="No file part in the request or invalid file type."
        )


# Route to get list of case files
@router.get("/get-case-files")
async def get_case_files_route():
    return await get_case_files()


# Route to get list of employee files
@router.get("/get-employee-files")
async def get_employee_files_route():
    return await get_employee_files()


# Route to get file columns
@router.get("/get-file-columns")
async def get_file_columns_route(
    file_name: str = Query(...), file_type: str = Query(...)
):
    return await get_file_columns(file_name, file_type)


# Route to get unique values for a column in a specific file
@router.post("/get-column-values")
async def get_column_values_route(data: ColumnValuesRequest):
    return await get_column_values(data)


# Route to process files
@router.post("/process")
async def process_files_route(request: Request):
    return await process_files(request)


# Route to upload to DB
@router.post("/upload-to-db")
async def upload_to_db_route(request: UploadToDBRequest):
    return await upload_to_db(request)
