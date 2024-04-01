FROM apache/airflow:2.8.2-python3.11

WORKDIR /opt/airflow

USER root

# Install Open JDK to allow Apache Beam to use embedded job server for Flink and Spark runners.
# Note: Debian Bullseye repository is added in order to have Open JDK version 11 because Debian Bookworm repository
# contains only Open JDK version 17 that is not compatible with Apache Beam.
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository 'deb http://deb.debian.org/debian/ bullseye main' && \
    apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean

# Install snappy library and build-essential tools because GCC is neeeded to build package python-snappy.
RUN apt-get update && \
    apt-get install -y build-essential libsnappy-dev && \
    apt-get clean

USER airflow

COPY --chown=airflow:root ./infrastructure/orchestrator/requirements.txt ./airflow-requirements.txt
COPY --chown=airflow:root ./infrastructure/executor/requirements.txt ./executor-requirements.txt
COPY --chown=airflow:root ./infrastructure/executor/extra-requirements.txt .

RUN pip install --no-cache-dir -r ./airflow-requirements.txt -r ./executor-requirements.txt -r ./extra-requirements.txt
