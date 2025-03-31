FROM bitnami/spark:3.3

WORKDIR /app

# Install evaluation dependencies
RUN pip install pandas pyarrow

# Copy evaluation scripts
COPY evaluate.py .

# Set entry point
CMD ["python", "evaluate.py"]