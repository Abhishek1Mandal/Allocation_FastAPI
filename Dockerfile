# Use official Python image
FROM python:3.13-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies (optional but often needed)
# You can skip if your app doesn't need build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app files
COPY . .

# Expose the port uvicorn will run on (optional but good practice)
EXPOSE 5050

# Run the FastAPI app
CMD ["uvicorn", "main:app", "--host=0.0.0.0", "--port=5050",  "--reload"]
