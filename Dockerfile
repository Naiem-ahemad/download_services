# Use lightweight Python base
FROM python:3.11-slim

# Install ffmpeg and aria2
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    aria2 \
 && rm -rf /var/lib/apt/lists/*

# Set work directory
WORKDIR /app

# Install pipenv/requirements if needed
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Expose port for Cloud Run
ENV PORT=8080

# Run the FastAPI app
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
