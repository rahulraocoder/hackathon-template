# Use official Python image with build tools
FROM python:3.9

# Install Java for Spark (using OpenJDK 17)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    rm -rf /var/lib/apt/lists/*
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture)" >> /root/.bashrc
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY . .

# Entry point
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
