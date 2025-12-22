# Use Python 3.14 slim image
FROM python:3.14-slim

# Set timezone to Australia/Adelaide
ENV TZ=Australia/Adelaide
RUN apt-get update && apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application directories and files
COPY orbe_etl/src/ ./src/
COPY orbe_etl/img/ ./img/
COPY orbe_etl/generate_weekly_reports.py .

# Create tmp directory for output files
RUN mkdir -p tmp

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the weekly reports generation script
CMD ["python", "generate_weekly_reports.py"]
