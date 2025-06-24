from io import StringIO
from fastapi import APIRouter, File, UploadFile, HTTPException, Body
from pydantic import BaseModel, EmailStr
import pandas as pd
import sys
import os

# Add the parent directory to Python path to access Main module
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Now import from Main
from Main.credentialsService import collection
from Main.credentialsService import generate_password_from_name, convert_to_mongodb_binary, hash_password_bcrypt
from Main.credentialsService import create_employee
import logging

# Define EmployeeIn directly here to avoid import issues
class EmployeeIn(BaseModel):
    E_ID: int
    E_Name: str
    email: EmailStr
    address1: str
    address2: str
    role: str
    mobile: int
    altMobile: int
    latitude: float
    longitude: float
    physicalAddress: str
    userStatus: str

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
    try:
        # Basic file validation
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
        
        # Read file content
        contents = await file.read()
        file_str = contents.decode("utf-8")
        csv_file = StringIO(file_str)
        df = pd.read_csv(csv_file)
        
        logger.info(f"CSV loaded with {len(df)} rows")
        logger.info(f"Original columns: {list(df.columns)}")
        
        # Clean column names - remove extra spaces and standardize
        df.columns = df.columns.str.strip()
        
        logger.info(f"Cleaned columns: {list(df.columns)}")
        
        # Check required columns
        required_columns = {
            "E_ID", "E_Name", "email", "address1", "address2",
            "role", "mobile", "altMobile", "latitude",
            "longitude", "physicalAddress", "userStatus"
        }
        
        # Check if all required columns exist
        missing_columns = []
        for col in required_columns:
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            available_cols = list(df.columns)
            raise HTTPException(
                status_code=400, 
                detail=f"Missing columns: {missing_columns}. Available columns: {available_cols}. Please check your CSV format."
            )
        
        employee_summaries = []
        failed_rows = []
        
        for index, row in df.iterrows():
            try:
                # Simple data conversion
                row_data = {
                    "E_ID": int(row["E_ID"]) if pd.notna(row["E_ID"]) else 0,
                    "E_Name": str(row["E_Name"]).strip() if pd.notna(row["E_Name"]) else "",
                    "email": str(row["email"]).strip() if pd.notna(row["email"]) else "",
                    "address1": str(row["address1"]).strip() if pd.notna(row["address1"]) else "",
                    "address2": str(row["address2"]).strip() if pd.notna(row["address2"]) else "",
                    "role": str(row["role"]).strip() if pd.notna(row["role"]) else "",
                    "mobile": int(float(row["mobile"])) if pd.notna(row["mobile"]) and str(row["mobile"]).strip() != "" else 0,
                    "altMobile": int(float(row["altMobile"])) if pd.notna(row["altMobile"]) and str(row["altMobile"]).strip() != "" else 0,
                    "latitude": float(row["latitude"]) if pd.notna(row["latitude"]) and str(row["latitude"]).strip() != "" else 0.0,
                    "longitude": float(row["longitude"]) if pd.notna(row["longitude"]) and str(row["longitude"]).strip() != "" else 0.0,
                    "physicalAddress": str(row["physicalAddress"]).strip() if pd.notna(row["physicalAddress"]) else "",
                    "userStatus": str(row["userStatus"]).strip() if pd.notna(row["userStatus"]) else ""
                }
                
                # Basic validation
                if row_data["E_ID"] == 0:
                    raise ValueError("E_ID cannot be empty or zero")
                if not row_data["E_Name"]:
                    raise ValueError("E_Name cannot be empty")
                if not row_data["email"]:
                    raise ValueError("email cannot be empty")
                
                # Create employee
                employee_data = EmployeeIn(**row_data)
                result = await create_employee(employee_data)
                employee_summaries.append(result)
                logger.info(f"Successfully processed row {index + 1}")
                
            except Exception as e:
                error_msg = str(e)
                logger.error(f"Error processing row {index + 1}: {error_msg}")
                failed_rows.append({
                    "row": index + 1,
                    "error": error_msg,
                    "data": dict(row) if hasattr(row, 'to_dict') else str(row)
                })
        
        return {
            "message": f"Processed {len(df)} rows: {len(employee_summaries)} successful, {len(failed_rows)} failed",
            "employee_summaries": employee_summaries,
            "failed_rows": failed_rows
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"CSV Upload Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Server error: {str(e)}")

# Forgot Password API
@router.post("/forgot-password")
async def forgot_password(
    E_Name: str = Body(..., embed=True),
    E_ID: int = Body(..., embed=True)
):
    try:
        E_Name_cleaned = " ".join(E_Name.strip().lower().split())
        
        employee = await collection.find_one({
            "E_Name": {"$regex": f"^{E_Name_cleaned}$", "$options": "i"},
            "E_ID": E_ID
        })
        
        if not employee:
            raise HTTPException(status_code=404, detail="Employee not found")
        
        # Generate new password using name_XXX format
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
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Password reset failed for {E_Name} (ID: {E_ID}): {e}")
        raise HTTPException(status_code=500, detail="Failed to reset password")