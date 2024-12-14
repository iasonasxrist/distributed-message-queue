# First stage: Build the Python environment and install dependencies
FROM python:3.12-slim AS builder

WORKDIR /app

# Copy requirements file
COPY ./ .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

EXPOSE 5000

# Run app.py when the container launches
CMD ["python", "-m", "broker.broker"]