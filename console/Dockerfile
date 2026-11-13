FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the console service and its page
COPY console.py index.html /app/

# Install dependencies
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" kazoo requests

# Expose the console port
EXPOSE 8080

# Command to run the console
CMD ["uvicorn", "console:app", "--host", "0.0.0.0", "--port", "8080", "--no-access-log"]
