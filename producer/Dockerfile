# First stage: Build the Python environment and install dependencies
FROM python:3.12-slim AS builder

WORKDIR /app

# Copy requirements file
COPY . /app

ENV PYTHONPATH=/app

RUN pip install --no-cache-dir -r producer/requirements.txt

EXPOSE 5000

# Run app.py when the container launches
CMD ["python3", "-m", "producer.producer"]