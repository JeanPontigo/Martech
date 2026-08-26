FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY extract_gcp_carozzi.py .
COPY main_gcp_carozzi.py .
COPY utils_gcp_carozzi.py .

CMD ["python", "main_gcp_carozzi.py"]
