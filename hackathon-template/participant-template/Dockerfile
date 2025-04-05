FROM python:3.9-slim

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir pandas requests

CMD ["python", "pipeline/main.py"]
