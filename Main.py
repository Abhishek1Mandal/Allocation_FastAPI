from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from Routes.AllocationDashboardRoutes import router as allocation_router
from Routes.GPSCoordinateRoutes import router as gps_router
from Routes.LoanNumberRoutes import router as loan_router
from Routes.LoanNumberMaskForManualRoutes import router as manual_loan_mask_router
from Routes.GPSDistanceRoutes import router as gps_distance_router
from Routes.LoanProcessingRoutes import router as loan_processing_router
from Routes.ExcelUploadRoutes import router as excel_upload_router

app = FastAPI()

# Configure CORS for local Wi-Fi and frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://192.168.1.3:5173",  # Your machine's frontend
        "http://192.168.1.4:5173",  # Other devices on Wi-Fi (add more as needed)
        "*",  # Temporary for testing across Wi-Fi
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include the routes
app.include_router(allocation_router)
app.include_router(gps_router, prefix="/gps")
app.include_router(loan_router, prefix="/loan")
app.include_router(manual_loan_mask_router, prefix="/manualloanmask")
app.include_router(gps_distance_router, prefix="/gps-distance")
app.include_router(loan_processing_router, prefix="/loan-processing")
app.include_router(excel_upload_router, prefix="/excel-upload")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5050)
