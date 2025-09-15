# Use CUDA-enabled base image with Python
FROM nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    ffmpeg \
    libgl1 \
    git \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set container working directory to match mounted project
WORKDIR /workspace

# Upgrade pip
RUN pip3 install --upgrade pip

# Install requirements (done later so it can use the host’s Miopoint repo)
# We don’t copy source; requirements will be installed from the mounted folder
# This way you can keep developing in the current directory
# and avoid rebuilding the image when you change code
RUN echo "Miopoint will install requirements from the mounted folder at runtime"

EXPOSE 8000

# Default command: run Miopoint in the current folder
CMD pip3 install -r requirements.txt && \
    python3 main.py --listen 0.0.0.0 --port 8000
