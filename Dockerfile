FROM apache/airflow:2.9.3

# Switch to root user
USER root

# Install gcc and other necessary build tools ( /!\ Mandatory for apache airflow packages)
RUN apt-get update && apt-get install -y gcc python3-dev

# Install Poetry
RUN pip3 install poetry

# Copy the pyproject.toml and poetry.lock files if available
COPY pyproject.toml .
COPY poetry.lock .

# Export Poetry dependencies to requirements.txt
RUN poetry export -f requirements.txt --output requirements.txt

# Install dependencies using pip
RUN pip3 install -r requirements.txt

# Clean up to reduce image size
RUN apt-get remove -y gcc python3-dev && apt-get autoremove -y && apt-get clean
