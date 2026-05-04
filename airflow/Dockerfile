FROM apache/airflow:3.2.1
USER root
RUN apt-get update && apt-get install -y default-jdk
ENV JAVA_HOME=/usr/lib/jvm/default-java
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt