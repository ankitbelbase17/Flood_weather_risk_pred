FROM apache/airflow:2.7.2-python3.8
ADD requirements.txt .
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
RUN export JAVA_HOME
RUN pip install -r requirements.txt
