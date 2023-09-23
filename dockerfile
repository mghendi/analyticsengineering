# Use an official Ubuntu as a parent image
FROM ubuntu:latest

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive

# Update the package list and install required packages
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    awscli \
    postgresql-client \
    && apt-get clean

# Install dbt-core
RUN pip3 install dbt

# Install boto3
RUN pip3 install boto3

# Set up a working directory (optional)
WORKDIR /app

# Define the default command to run when the container starts
CMD ["bash"]