# The image of the container that joins as a backend: Python, Java for
# Spark's local mode, and the pipeline's libraries. Nothing of Zenthros is in
# it: the agent is installed when the container joins.
#
# The Python minor (3.13) must be the notebook's: a function pickled there is
# unpickled here.
FROM python:3.13-slim-bookworm

# Java for Spark; curl and ca-certificates for the agent's installer; procps
# because Spark's scripts call ps.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates procps \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/pipeline/requirements.txt
RUN pip install --no-cache-dir -r /opt/pipeline/requirements.txt

# Spark's S3 connector, downloaded and checked now so a job downloads nothing.
COPY spark_jars.py /opt/pipeline/spark_jars.py
RUN python /opt/pipeline/spark_jars.py

# Jobs run as an ordinary user: the agent needs no root and gets none.
RUN useradd --create-home --uid 1000 zenthros
USER zenthros
WORKDIR /home/zenthros
ENV PYTHONUNBUFFERED=1
