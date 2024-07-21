FROM apache/airflow:2.7.3-python3.11

# Switch to root user
USER root

# Install gcc and other necessary build tools ( /!\ Mandatory for apache airflow packages)
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    curl \
    gnupg \
    ca-certificates \
    apt-transport-https

# Install Google Cloud SDK
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update && apt-get install -y google-cloud-sdk
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
