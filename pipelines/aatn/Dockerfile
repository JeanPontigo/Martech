FROM python:3.11-slim
WORKDIR /app
COPY requirements_aatn.txt .
RUN pip install --no-cache-dir -r requirements_aatn.txt
COPY extract_gcp_ariztia.py .
COPY main_gcp_ariztia.py .
COPY utils_gcp_ariztia.py .
ENV PYTHONUNBUFFERED=1
CMD ["python3", "main_gcp_ariztia.py", "--entity", "all"]
