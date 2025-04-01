FROM bitnami/spark:3.3

WORKDIR /app

# Install evaluation dependencies
RUN pip install pandas pyarrow py4j

# Copy evaluation scripts
COPY evaluate.py .
COPY pipeline.py .
COPY utils.py .

# Set entry point
# Keep container running after execution
CMD ["tail", "-f", "/dev/null"]