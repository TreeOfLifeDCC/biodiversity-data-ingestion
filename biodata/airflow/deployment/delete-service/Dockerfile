FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# IMPORTANT: target must match the function name in main.py
CMD ["functions-framework", "--target=delete_composer_environment", "--port=8080"]