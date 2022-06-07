FROM gcr.io/datamechanics/spark:platform-3.2.1-latest
MAINTAINER asi@dbca.wa.gov.au
LABEL org.opencontainers.image.source https://github.com/dbca-wa/pyspark-docker

WORKDIR /opt/application/

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN pip3 install azure-storage-blob==12.11.0 azure-identity==1.10.0

COPY utils.py oracle_financials_logins.py http_requests_single_host.py http_requests_ip.py ./
