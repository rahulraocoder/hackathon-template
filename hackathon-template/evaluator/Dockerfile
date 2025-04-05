FROM python:3.9-slim

# Install sqlite3 and create data directory
RUN apt-get update && apt-get install -y sqlite3 && \
    mkdir -p /data && chmod a+rwx /data

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
