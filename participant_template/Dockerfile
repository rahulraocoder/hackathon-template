FROM bitnami/spark:3.3

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy all participant files
COPY . .

# Set memory limits
ENV SPARK_DRIVER_MEMORY=1g
ENV SPARK_EXECUTOR_MEMORY=1g

# Entry point
CMD ["spark-submit", "--master", "spark://spark-master:7077", "/app/pipeline.py"]