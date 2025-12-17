# Al-Mudeer Backend Dockerfile
# Optimized for Railway deployment

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies globally
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Railway uses PORT environment variable (default 8000)
ENV PORT=8000
ENV PYTHONUNBUFFERED=1

# Expose port
EXPOSE 8000

# Run the application
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
