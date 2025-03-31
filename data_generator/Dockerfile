FROM python:3.9-slim

WORKDIR /app

# Install dependencies
RUN pip install pandas pyarrow faker

# Copy data generation script
COPY generate_data.py .

# Set entry point
CMD ["python", "generate_data.py"]