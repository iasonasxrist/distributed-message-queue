FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the auth service
COPY auth.py /app/

# Install dependencies
RUN pip install --no-cache-dir fastapi "uvicorn[standard]" kazoo

# Expose the auth port
EXPOSE 7000

# Command to run the auth service
CMD ["uvicorn", "auth:app", "--host", "0.0.0.0", "--port", "7000", "--no-access-log"]
