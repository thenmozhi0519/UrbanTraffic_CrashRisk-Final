FROM python:3.10-slim

# Avoid buffering issues
ENV PYTHONUNBUFFERED=1

WORKDIR /workspace

# System dependencies (needed for PySpark)
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Java setup for PySpark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Upgrade pip
RUN pip install --upgrade pip

# 👇 Copy requirements first (important for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir --default-timeout=200 -r requirements.txt

# Copy full project
COPY . .

# Keep container alive
CMD ["tail", "-f", "/dev/null"]