FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y default-jre wget libpq-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars && \
    wget -qO /opt/spark/jars/ojdbc8.jar https://repo1.maven.org/maven2/com/oracle/database/jdbc/ojdbc8/21.1.0.0/ojdbc8-21.1.0.0.jar

ENV SPARK_HOME="/usr/local/lib/python3.10/site-packages/pyspark"
ENV PYTHONPATH="${PYTHONPATH}:/app"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]