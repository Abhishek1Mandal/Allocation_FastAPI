from fastapi import APIRouter, UploadFile, File
from fastapi.responses import StreamingResponse
from Main.GPSDistanceCalculate import calculate_distances
import io

router = APIRouter()


@router.post("/calculate-distance")
async def calculate_distance(file: UploadFile = File(...)):
    # Check if the uploaded file is an Excel file
    if not file.filename.endswith(".xlsx"):
        return {"error": "Only .xlsx files are supported"}

    # Process the file
    df = calculate_distances(file.file)

    # Save the DataFrame to a BytesIO object
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    # Return the file as a streaming response
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename=processed_{file.filename}"
        },
    )
