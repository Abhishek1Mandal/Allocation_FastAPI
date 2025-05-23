import random
import bcrypt
from bson.binary import Binary
from datetime import datetime
from fastapi import  HTTPException
from schemas import EmployeeIn
from schemas import EmployeeIn
import logging
from dotenv import load_dotenv
import os
from motor.motor_asyncio import AsyncIOMotorClient
# Logging Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def generate_password_from_name(E_Name: str) -> str:
    """Generate password in format: name_XXX where XXX is random 3 digits."""
    # Extract first name from full name
    name_parts =E_Name.strip().upper().split()
    first_name = name_parts[0] if name_parts else "user"
    
    # Generate random 3-digit number
    random_digits = random.randint(100, 999)
    
    return f"{first_name}_{random_digits}"
def hash_password_bcrypt(password: str) -> bytes:
    """Hash the password using bcrypt and return the bytes."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt)


def convert_to_mongodb_binary(hashed_password: bytes) -> Binary:
    """Convert hashed password to MongoDB Binary format for secure storage."""
    return Binary(hashed_password)


def generate_username(E_Name: str, E_ID: int) -> str:
    """Generate a unique username based on employee's name and ID."""
    E_Name_cleaned = " ".join(E_Name.strip().lower().split())
    name_parts = E_Name_cleaned.split()
    first_name = name_parts[0] if name_parts else "user"
    return f"{first_name}{E_ID}".lower()

# Create single employee record
async def create_employee(emp: EmployeeIn) -> dict:
    try:
        username = generate_username(emp.E_Name, emp.E_ID)
        
        # Check username uniqueness
        existing_user = await collection.find_one({"username": username})
        if existing_user:
            raise HTTPException(status_code=400, detail=f"Username {username} already exists")
        
        # Generate password using name_XXX format
        plain_password = generate_password_from_name(emp.E_Name)
        hashed_pw = hash_password_bcrypt(plain_password)
        encoded_password = convert_to_mongodb_binary(hashed_pw)

        doc = emp.dict()
        doc.update({
            "username": username,
            "password": encoded_password,
            "activeTimestamp": datetime.now().strftime("%d/%m/%Y, %I:%M:%S %p"),
            "currentDeviceID": "",
            "currentSession": ""
        })

        await collection.insert_one(doc)

        return {
            "E_ID": emp.E_ID,
            "E_Name": emp.E_Name,
            "email": emp.email,
            "userStatus": emp.userStatus,
            "Username": username,
            "Password": plain_password  # Returns the plain password for user reference
        }

    except HTTPException:
        # Re-raise HTTPException to preserve the status code and message
        raise
    except Exception as ex:
        logger.error(f"Error creating employee {emp.E_ID}: {ex}")
        raise HTTPException(status_code=500, detail="Internal server error while creating employee")

# Load environment variables from .env file
load_dotenv()

# Get configuration from .env
mongo_uri = os.getenv("ATLAS_MONGO_URI")
db_name = os.getenv("MONGO_DATABASE", "recoverEase")  # Default fallback if not set
collection_name = os.getenv("COLLECTION_NAME", "testUsers")  # Default fallback if not set

# Validate required environment variables
if not mongo_uri:
    raise ValueError("ATLAS_MONGO_URI not found in .env file")

# Try connecting to MongoDB
try:
    # Connect to MongoDB
    client = AsyncIOMotorClient(mongo_uri)
    
    # Use database and collection names from environment variables
    db = client[db_name]
    collection = db[collection_name]
    
    # Test connection (Note: server_info() is synchronous, use ping for async)
    async def test_connection():
        try:
            await client.admin.command('ping')
            print(f"MongoDB connection successful!")
            print(f"Connected to database: {db_name}")
            print(f"Using collection: {collection_name}")
            return True
        except Exception as e:
            print(f"MongoDB connection test failed: {e}")
            return False
    
    print("MongoDB client initialized successfully!")
    
except Exception as e:
    print(f"Failed to initialize MongoDB client: {e}")
    raise