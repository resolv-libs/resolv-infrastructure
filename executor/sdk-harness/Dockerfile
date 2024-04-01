FROM python:3.11

USER root

# Install snappy library and build-essential tools because GCC is neeeded to build package python-snappy.
RUN apt-get update && \
    apt-get install -y build-essential libsnappy-dev && \
    apt-get clean

COPY --chown=root:root ./beam ./beam
COPY --chown=root:root ./infrastructure/executor/requirements.txt .
COPY --chown=root:root ./infrastructure/executor/extra-requirements.txt .

ENV PYTHONPATH=./beam:${PYTHONPATH}

RUN pip install --no-cache-dir -r ./requirements.txt -r ./extra-requirements.txt
