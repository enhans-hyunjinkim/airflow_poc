FROM apache/airflow:3.0.6

# Copy requirements.txt to the container
COPY requirements.txt /requirements.txt

# Install dependencies from requirements.txt
RUN pip install -r /requirements.txt
