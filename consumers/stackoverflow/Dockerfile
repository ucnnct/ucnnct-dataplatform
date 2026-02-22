FROM python:3.11-slim
WORKDIR /app
COPY stackoverflow/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer_base.py .
COPY stackoverflow/main.py .
CMD ["python", "main.py"]
