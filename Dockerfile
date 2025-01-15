# Import older but known stable version of astronomer
FROM quay.io/astronomer/astro-runtime:8.8.0


# Set up soda virtual environment and install dependencies, used for data checks. Import older stable versions of Soda.
RUN python -m venv soda_venv && \
    source soda_venv/bin/activate && \
    pip install --no-cache-dir soda-core-bigquery==3.0.45 && \
    pip install --no-cache-dir soda-core-scientific==3.0.45 && \
    deactivate


# install dbt into a virtual environment
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --upgrade pip && \
    pip install --no-cache-dir dbt-bigquery && \
    deactivate

# Astro has permission issues with dbt. Need to create a new folder that can be interacted with.
RUN cp -r /usr/local/airflow/include/dbt /usr/local/airflow