FROM gcr.io/datamechanics/spark:platform-3.2.1-latest
MAINTAINER asi@dbca.wa.gov.au
LABEL org.opencontainers.image.source https://github.com/dbca-wa/pyspark-docker
ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /opt/application/
RUN wget -q -O /tmp/postgresql-42.5.0.jar https://jdbc.postgresql.org/download/postgresql-42.5.0.jar \
  && mv /tmp/postgresql-42.5.0.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY *.py ./
