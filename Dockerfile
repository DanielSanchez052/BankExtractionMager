# Use an official Python runtime as a parent image
FROM python:3.10-bullseye

# Install system dependencies
# poppler-utils is required for libraries like camelot-py and pdf2image
RUN apt-get update && apt-get install -y \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory in the container
WORKDIR /app

# Copy the dependencies file to the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code to the working directory
COPY . .

# Specify the command to run on container start
CMD ["python", "main.py"]
