# Select a base Python image
FROM python:3.8-slim

# Install dependencies
RUN pip install Flask requests pandas google-cloud-storage

# Copy your Python script into the container
COPY 5day_3hour_weather_forecast.py /app/5day_3hour_weather_forecast.py

# Set the working directory
WORKDIR /app

# Run the script with Flask or directly as a Python script
CMD ["python", "5day_3hour_weather_forecast.py"]
