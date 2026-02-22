FROM python:3.11-slim
WORKDIR /app
COPY hackernews/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY consumer_base.py .
COPY hackernews/main.py .
CMD ["python", "main.py"]
