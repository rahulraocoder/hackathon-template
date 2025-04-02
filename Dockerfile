# Use official Python image
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY . .

# Entry point
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
