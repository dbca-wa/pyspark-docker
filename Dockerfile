FROM gcr.io/datamechanics/spark:platform-3.2.1-latest

WORKDIR /opt/application/

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
RUN pip3 install azure-storage-blob==12.11.0 azure-identity==1.10.0

COPY oracle_financials_logins.py .
