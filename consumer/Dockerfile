FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the consumer script
COPY consumer.py /app/

# Install dependencies
RUN pip install fastapi "uvicorn[standard]" requests kazoo redis

# Expose the FastAPI server port
EXPOSE 5000

# Command to run the consumer service
CMD ["uvicorn", "consumer:app", "--host", "0.0.0.0", "--port", "5000", "--no-access-log"]
