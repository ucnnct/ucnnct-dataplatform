FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app2.py .

EXPOSE 8501

CMD ["streamlit", "run", "app2.py", "--server.address=0.0.0.0", "--server.port=8501"]
