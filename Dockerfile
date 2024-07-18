FROM apache/airflow:2.9.3
USER root
COPY pyproject.toml .
RUN pip3 install poetry \
    && poetry install
