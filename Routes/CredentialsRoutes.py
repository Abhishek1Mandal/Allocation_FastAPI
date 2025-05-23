
from io import StringIO
from fastapi import  APIRouter,File, UploadFile, HTTPException, Body
from schemas import EmployeeIn 
import pandas as pd 
from Main.credentialsService import collection 
from Main.credentialsService import generate_password_from_name,convert_to_mongodb_binary, hash_password_bcrypt
from Main.credentialsService import create_employee
import logging
router = APIRouter()
# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add employee via form
@router.post("/add-employee")
async def add_employee(emp: EmployeeIn):
    result = await create_employee(emp)
    return {
        "message": "Employee added successfully",
        "employee_summary": result
    }

# Bulk add via CSV  
@router.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")

    try:
        contents = await file.read()
        file_str = contents.decode("utf-8")
        csv_file = StringIO(file_str)
        df = pd.read_csv(csv_file)

        required_columns = {
            "E_ID", "E_Name", "email", "address1", "address2",
            "role", "mobile", "altMobile", "latitude",
            "longitude", "physicalAddress", "userStatus"
        }

        if not required_columns.issubset(df.columns):
            missing = required_columns - set(df.columns)
            raise HTTPException(status_code=400, detail=f"Missing columns: {missing}")

        employee_summaries = []
        failed_rows = []

        for index, row in df.iterrows():
            try:
                employee_data = EmployeeIn(**row.to_dict())
                result = await create_employee(employee_data)
                employee_summaries.append(result)
            except Exception as e:
                logger.warning(f"Failed to process row {index + 1}: {e}")
                failed_rows.append({"row": index + 1, "error": str(e)})

        return {
            "message": "CSV processed",
            "employee_summaries": employee_summaries,
            "failed_rows": failed_rows
        }

    except Exception as e:
        logger.error(f"CSV Upload Error: {e}")
        raise HTTPException(status_code=500, detail="Failed to process CSV file")

# Forgot Password API
@router.post("/forgot-password")
async def forgot_password(
    E_Name: str = Body(..., embed=True),
    E_ID: int = Body(..., embed=True)
):
    E_Name_cleaned = " ".join(E_Name.strip().lower().split())

    employee = await collection.find_one({
        "E_Name": {"$regex": f"^{E_Name_cleaned}$", "$options": "i"},
        "E_ID": E_ID
    })

    if not employee:
        raise HTTPException(status_code=404, detail="Employee not found")

    try:
        # Generate new password using name_XXX format (same name, new 3 digits)
        new_plain_password = generate_password_from_name(employee["E_Name"])
        hashed_password_bytes = hash_password_bcrypt(new_plain_password)
        base64_encoded_password = convert_to_mongodb_binary(hashed_password_bytes)

        await collection.update_one(
            {"_id": employee["_id"]},
            {"$set": {"password": base64_encoded_password}}
        )

        return {
            "message": "Password reset successfully.",
            "username": employee.get("username"),
            "new_plain_password": new_plain_password
        }
    except Exception as e:
        logger.error(f"Password reset failed for {E_Name} (ID: {E_ID}): {e}")
        raise HTTPException(status_code=500, detail="Failed to reset password")
 