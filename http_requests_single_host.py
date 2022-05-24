import argparse
import csv
from datetime import datetime, timedelta
from log4j4py import Log4JProxyHandler
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
import sys


STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("HttpRequestsSingleHost")


def spark_session():
    """Get and return a Spark session.
    """
    session = SparkSession.builder.getOrCreate()
    session.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", STORAGE_ACCOUNT_KEY)
    session.conf.set("network.timeout", 1200)
    return session


def read_nginx_logs(hours_ago, session, blob_container="access-logs"):
    """Read Nginx logs from blob storage for a given number of hours into the past
    and parse them into a Spark session, returning a DataFrame.
    """
    schema = StructType(fields=[
        StructField("timestamp", StringType(), True),
        StructField("remote_ip", StringType(), True),
        StructField("host", StringType(), True),
        StructField("path", StringType(), True),
        StructField("params", StringType(), True),
        StructField("method", StringType(), True),
        StructField("protocol", StringType(), True),
        StructField("status", IntegerType(), True),
        StructField("request_time", IntegerType(), True),
        StructField("bytes_sent", IntegerType(), True),
        StructField("user_agent", StringType(), True),
        StructField("email", StringType(), True),
    ])

    filename = 'wasbs://{}@{}.blob.core.windows.net/{}.nginx.access.csv'
    file_list = []
    t = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)  # Start at the beginning of the current date.

    for i in range(hours_ago):
        csv_path = filename.format(blob_container, STORAGE_ACCOUNT_NAME, t.strftime("%Y%m%d%H"))
        file_list.append(csv_path)
        t = t - timedelta(hours=1)

    df = session.read.options(mode="DROPMALFORMED").load(file_list, format='csv', schema=schema)
    df = df.withColumn("timestamp", df.timestamp[0:19])  # Slice off the timezone string.
    # Re-cast the timestamp column type.
    df = df.withColumn("timestamp", df.timestamp.cast(TimestampType()))

    return df


def exclude_requests(df):
    # Exclude requests from Nessus
    df = df.filter(df.remote_ip != "10.6.20.97")
    # Exclude requests from PRTG
    df = df.filter(df.user_agent != "Mozilla/5.0 (compatible; PRTG Network Monitor (www.paessler.com); Windows)")
    return df


def filter_requests(df, host):
    """Apply filters to the DataFrame to only include requests to host of interest.
    """
    df = df.filter((df.host == host) & (df.email != ""))
    df = df.orderBy(df.timestamp)
    return df


def write_report(df, filename):
    """For the passed-in DataFrame, write out the contents to a CSV.
    """
    df = df.coalesce(1)
    temp_file = open(f"/out/{filename}", "w+")
    fieldnames = df.columns
    writer = csv.DictWriter(temp_file, fieldnames)
    writer.writerow(dict(zip(fieldnames, fieldnames)))

    for row in df.toLocalIterator():
        writer.writerow(row.asDict())


if __name__ == "__main__":
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--hours", action="store", type=int, required=True, help="Number of hours into the past to load Nginx logs")
    all_args.add_argument("--host", action="store", type=str, required=True, help="Hostname to filter HTTP request for")
    all_args.add_argument("--filename", action="store", type=str, required=True, help="Filename for the CSV report output")
    args = vars(all_args.parse_args())
    hours_ago = int(args["hours"])
    host = str(args["host"])
    filename = str(args["filename"])
    session = spark_session()
    pyspark_handler = Log4JProxyHandler(session)
    LOGGER.addHandler(pyspark_handler)
    LOGGER.info("Starting report generation")
    df = read_nginx_logs(hours_ago, session)
    df = exclude_requests(df)
    df = filter_requests(df, host)
    write_report(df, filename)
