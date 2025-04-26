from fastapi import HTTPException
import pandas as pd
import numpy as np
import math
import os
from pymongo import MongoClient
from dotenv import load_dotenv
import openpyxl
from datetime import datetime
import pytz
import json
import shutil

# Load environment variables from .env file
load_dotenv()

# MongoDB configuration from .env
uri = os.getenv("ATLAS_MONGO_URI")
validPassword = os.getenv("VALID_PASSWORD")
db_name = os.getenv("MONGO_DATABASE")  # Database name from .env
fos_collection_name = os.getenv("FOS_COLLECTION_NAME")  # Field Officers collection
cases_collection_name = os.getenv("CASES_COLLECTION_NAME")  # Borrower Cases collection
assignments_collection_name = os.getenv("MONGO_COLLECTION")  # Assigned cases collection

# Initialize MongoDB client
client = MongoClient(uri)
db = client[db_name]
collection_fos = db[fos_collection_name]
collection_cases = db[cases_collection_name]
collection_assignments = db[assignments_collection_name]


# Configuration for file upload
def secure_filename(filename: str) -> str:
    return "".join(c for c in filename if c.isalnum() or c in (".", "_", "-")).rstrip()


UPLOAD_FOLDER = "./files"
EMPLOYEES_FOLDER = os.path.join(UPLOAD_FOLDER, "employees")
CASES_FOLDER = os.path.join(UPLOAD_FOLDER, "cases")
ALLOWED_EXTENSIONS = {"xlsx"}

# Ensure the directories exist
os.makedirs(EMPLOYEES_FOLDER, exist_ok=True)
os.makedirs(CASES_FOLDER, exist_ok=True)


# Check allowed file extension
def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# Haversine function to calculate distance
def haversine_distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R = 6371  # Radius of the Earth in kilometers
    return R * c


async def get_case_files():
    case_files = os.listdir(CASES_FOLDER)
    return case_files


async def get_employee_files():
    employee_files = os.listdir(EMPLOYEES_FOLDER)
    return employee_files


async def get_file_columns(file_name: str, file_type: str):
    if not file_name:
        raise HTTPException(status_code=400, detail="File name is required.")
    if not file_type:
        raise HTTPException(status_code=400, detail="File type is required")

    folder_path = (
        EMPLOYEES_FOLDER
        if file_type == "emp"
        else CASES_FOLDER if file_type == "case" else None
    )
    if not folder_path:
        raise HTTPException(status_code=400, detail="Invalid file type.")

    file_path = os.path.join(folder_path, file_name)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")

    try:
        df = pd.read_excel(file_path)
        columns = df.columns.tolist()
        columns = [col for col in columns if col not in ["latitude", "longitude"]]
        return {"columns": columns}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Failed to extract columns: {str(e)}"
        )


async def get_column_values(data):
    file_name = data.file_name
    column_name = data.column_name
    file_type = data.file_type

    if not file_name or not column_name or not file_type:
        raise HTTPException(
            status_code=400, detail="File name, column name, or file type is missing"
        )

    folder_path = (
        EMPLOYEES_FOLDER
        if file_type == "emp"
        else CASES_FOLDER if file_type == "case" else None
    )
    if not folder_path:
        raise HTTPException(status_code=400, detail="Invalid file type")

    try:
        file_path = os.path.join(folder_path, file_name)
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="File not found")

        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active
        column_index = None
        for idx, cell in enumerate(sheet[1]):
            if cell.value == column_name:
                column_index = idx + 1
                break

        if column_index is None:
            raise HTTPException(status_code=404, detail="Column not found in file")

        unique_values = set()
        for row in sheet.iter_rows(
            min_row=2, min_col=column_index, max_col=column_index
        ):
            unique_values.add(row[0].value)

        sorted_unique_values = sorted(filter(None, unique_values))
        if None in unique_values:
            sorted_unique_values.insert(0, None)

        return sorted_unique_values
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def process_files(request):
    form = await request.form()
    if "employee_file" not in form or "case_file" not in form:
        raise HTTPException(status_code=400, detail="Both files are required!")

    MAX_CASES = form["max_cases"]
    try:
        MAX_CASES = int(MAX_CASES)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid value for MAX_CASES. It must be a number greater than 0.",
        )

    fos_file = form["employee_file"]
    master_file = form["case_file"]

    # Optional filters
    employee_filters = form.get("employee_filters", [])
    if isinstance(employee_filters, str):
        employee_filters = json.loads(employee_filters)

    case_filters = form.get("case_filters", [])
    if isinstance(case_filters, str):
        case_filters = json.loads(case_filters)

    fos_file_path = os.path.join(EMPLOYEES_FOLDER, fos_file)
    master_file_path = os.path.join(CASES_FOLDER, master_file)

    def keep_only_existing_columns(dataframe, columns_to_keep):
        existing_columns = [col for col in columns_to_keep if col in dataframe.columns]
        return dataframe[existing_columns]

    fos_columns_to_keep = [
        "E_Name",
        "E_ID",
        "role",
        "activeStatus",
        "physicalAddress",
        "latitude",
        "longitude",
    ]
    fos_data = pd.read_excel(fos_file_path)
    fos_data = keep_only_existing_columns(fos_data, fos_columns_to_keep)

    master_columns_to_keep = [
        "LoanNo/CC",
        "Lot",
        "Port",
        "BKT/DPD",
        "Asset/Product",
        "Cus_Name",
        "Cus_Mobile",
        "Cus_Add",
        "Mailing_Loc",
        "District",
        "Perma_Add",
        "Emp_Address",
        "latitude",
        "longitude",
        "EMI",
        "TAD",
        "POS",
        "TC_ID",
        "TC_Name",
        "TL_ID",
        "TL_Name",
        "assignedStatus",
        "Masked_LoanNo/CC",
    ]
    master_data = pd.read_excel(master_file_path)
    master_data = keep_only_existing_columns(master_data, master_columns_to_keep)

    for filter_item in employee_filters:
        column = filter_item.get("column")
        values = filter_item.get("values", [])
        if column and values:
            fos_data = fos_data[fos_data[column].isin(values)]

    for filter_item in case_filters:
        column = filter_item.get("column")
        values = filter_item.get("values", [])
        if column and values:
            master_data = master_data[master_data[column].isin(values)]

    fos_data["E_Name"] = fos_data["E_Name"].str.strip()

    if "latitude" not in fos_data.columns or "longitude" not in fos_data.columns:
        raise HTTPException(
            status_code=400,
            detail="Employee file must have latitude and longitude columns",
        )

    if (
        "latitude" not in master_data.columns
        or "longitude" not in master_data.columns
        or "assignedStatus" not in master_data.columns
    ):
        raise HTTPException(
            status_code=400,
            detail="Case file must have latitude, longitude, and assignedStatus columns",
        )

    fos_data["latitude"] = pd.to_numeric(fos_data["latitude"], errors="coerce")
    fos_data["longitude"] = pd.to_numeric(fos_data["longitude"], errors="coerce")
    master_data["latitude"] = pd.to_numeric(master_data["latitude"], errors="coerce")
    master_data["longitude"] = pd.to_numeric(master_data["longitude"], errors="coerce")

    fos_data = fos_data.dropna(subset=["latitude", "longitude"])
    master_data = master_data.dropna(subset=["latitude", "longitude", "assignedStatus"])
    master_data = master_data[
        master_data["assignedStatus"].str.contains("unAssigned", case=False)
    ]
    master_data.reset_index(drop=True, inplace=True)

    fos_locations = fos_data[["latitude", "longitude"]].values
    case_locations = master_data[["latitude", "longitude", "assignedStatus"]].values

    fos_assignment_count = {row["E_Name"]: 0 for _, row in fos_data.iterrows()}
    case_assignments = []
    distance_assignments = []
    unassigned_cases = []
    fos_case_distances = {fos: [] for fos in fos_data["E_Name"]}

    distance_matrix = np.zeros((len(case_locations), len(fos_locations)))
    for i, case in enumerate(case_locations):
        for j, fos in enumerate(fos_locations):
            distance_matrix[i, j] = haversine_distance(case[0], case[1], fos[0], fos[1])

    for i in range(len(case_locations)):
        case = case_locations[i]
        assigned_status = case[2]

        if "unAssigned" in assigned_status:
            sorted_fos_indices = np.argsort(distance_matrix[i])
            assigned = False

            for fos_index in sorted_fos_indices:
                fos_name = fos_data.iloc[fos_index]["E_Name"]
                fos_id = fos_data.iloc[fos_index]["E_ID"]
                if fos_assignment_count[fos_name] < MAX_CASES:
                    case_assignments.append({"FOS_Name": fos_name, "FOS_ID": fos_id})
                    distance_assignments.append(round(distance_matrix[i, fos_index], 3))
                    fos_assignment_count[fos_name] += 1
                    fos_case_distances[fos_name].append(
                        (distance_matrix[i, fos_index], i)
                    )
                    suffix = assigned_status.replace("unAssigned", "")
                    new_assigned_status = (
                        f"Assigned{int(suffix) + 1}"
                        if suffix.isdigit()
                        else "Assigned1"
                    )
                    master_data.at[i, "assignedStatus"] = new_assigned_status
                    assigned = True
                    break
                else:
                    case_assignments.append({"FOS_Name": None, "FOS_ID": None})
                    distance_assignments.append(None)
                    unassigned_cases.append(i)
                    break

    for case_index in unassigned_cases:
        sorted_fos_indices = np.argsort(distance_matrix[case_index])
        case = case_locations[case_index]
        assigned_status = case[2]

        for fos_index in sorted_fos_indices:
            fos_name = fos_data.iloc[fos_index]["E_Name"]
            fos_id = fos_data.iloc[fos_index]["E_ID"]
            if fos_assignment_count[fos_name] < MAX_CASES:
                case_assignments[case_index] = {"FOS_Name": fos_name, "FOS_ID": fos_id}
                suffix = assigned_status.replace("unAssigned", "")
                new_assigned_status = (
                    f"Assigned{int(suffix) + 1}" if suffix.isdigit() else "Assigned1"
                )
                master_data.at[case_index, "assignedStatus"] = new_assigned_status
                distance_assignments[case_index] = round(
                    distance_matrix[case_index, fos_index], 3
                )
                fos_assignment_count[fos_name] += 1
                break

    master_data["Assigned_FOS"] = [
        assignment["FOS_Name"] for assignment in case_assignments
    ]
    master_data["Assigned_FOS_ID"] = [
        assignment["FOS_ID"] for assignment in case_assignments
    ]
    master_data["Distance(KM)"] = distance_assignments

    excluded_cases = master_data[
        master_data["assignedStatus"].str.contains("unAssigned", case=False)
    ]
    excluded_cases.to_excel("Excluded_Cases.xlsx", index=False)

    master_data = master_data[
        ~master_data["assignedStatus"].str.contains("unAssigned", case=False)
    ]
    master_data.reset_index(drop=True, inplace=True)
    master_data = master_data.replace({np.nan: None})
    master_data["acceptanceStatus"] = "pending"

    response_data = {
        "fos_assignments": master_data.to_dict(orient="records"),
        "map_data": {
            "center_lat": fos_data["latitude"].mean(),
            "center_long": fos_data["longitude"].mean(),
            "fos_locations": [
                {
                    "lat": row["latitude"],
                    "long": row["longitude"],
                    "name": row["E_Name"],
                }
                for _, row in fos_data.iterrows()
            ],
            "case_locations": [
                {
                    "lat": row["latitude"],
                    "long": row["longitude"],
                    "Cus_Add": row["Cus_Add"],
                }
                for _, row in master_data.iterrows()
            ],
        },
    }

    return response_data


async def upload_to_db(request):
    try:
        data = request.data
        password = request.password

        if password != validPassword:
            raise HTTPException(status_code=403, detail="Invalid password.")

        if not data:
            raise HTTPException(status_code=400, detail="No data provided.")

        # Define the columns to keep (as specified)
        columns_to_keep = [
            "Assigned_FOS",
            "Assigned_FOS_ID",
            "BKT/DPD",
            "Cus_Add",
            "Cus_Mobile",
            "Cus_Name",
            "Distance(KM)",
            "EMI",
            "Emp_Address",
            "LoanNo/CC",
            "Masked_LoanNo/CC",
            "POS",
            "Perma_Add",
            "Port",
            "TAD",
            "TC_ID",
            "TC_Name",
            "TL_ID",
            "TL_Name",
            "Asset/Product",
            "acceptanceStatus",
            "assignedStatus",
        ]

        # Filter data to only include columns_to_keep
        filtered_data = [
            {key: doc[key] for key in columns_to_keep if key in doc} for doc in data
        ]

        # Add timestamp to filtered data
        indian_timezone = pytz.timezone("Asia/Kolkata")
        indian_timestamp = datetime.now(indian_timezone)
        for document in filtered_data:
            document["assignedTimestamp"] = indian_timestamp

        # Insert filtered data into MongoDB
        collection_assignments.insert_many(filtered_data)
        return {"message": "Data uploaded successfully."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"An error occurred while uploading data: {str(e)}"
        )
