FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Must match the function name in main.py
CMD ["functions-framework", "--target=cleanup_composer_bucket", "--port=8080"]
