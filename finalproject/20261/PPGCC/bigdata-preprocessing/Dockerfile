FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# src is mounted as a volume during development (edits reflected without rebuild).
# Copy is kept here so the image is self-contained for production runs.
COPY src/ ./src/
