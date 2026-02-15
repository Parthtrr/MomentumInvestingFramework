# Use official Python 3.9.6 image
FROM python:3.11.9

# Set the working directory inside the container
WORKDIR /app

# Copy the dependency file
COPY requirement.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirement.txt

# Copy the entire application into the container
COPY . .

# Set the default command to run the app
CMD ["python", "fullIndexing.py"]
