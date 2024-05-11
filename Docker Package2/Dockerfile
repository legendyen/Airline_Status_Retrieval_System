# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Install any needed packages specified in requirements.txt
RUN pip install requests pandas google-cloud-storage

# Copy the current directory contents into the container at /app
COPY . /app

# Run real_time_flights.py when the container launches
CMD ["python", "real_time_flights.py"]