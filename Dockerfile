FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy all files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install curl (needed in run.sh)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Make run.sh executable
RUN chmod +x run.sh

# Set PYTHONPATH
ENV PYTHONPATH=/app

CMD ["./run.sh"]
