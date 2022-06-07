import argparse
import csv
import logging
import os
from pyspark.sql.functions import hour
from pyspark.sql.types import DateType
import sys

from utils import Log4JProxyHandler, spark_session, read_nginx_logs


STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("HttpRequestsSingleIp")


def exclude_requests(df):
    # Exclude requests from Nessus
    df = df.filter(df.remote_ip != "10.6.20.97")
    # Exclude requests from PRTG
    df = df.filter(df.user_agent != "Mozilla/5.0 (compatible; PRTG Network Monitor (www.paessler.com); Windows)")
    return df


def process_dataframe(df):
    # Make a new column "hour", using the timestamp.
    df = df.withColumn("hour", hour("timestamp")).filter("hour is not null")
    # Re-cast the timestamp column to date and rename it.
    df = df.withColumn("timestamp", df.timestamp.cast(DateType()))
    df = df.withColumnRenamed("timestamp", "date")
    # Select just the columns of interest.
    df = df.select(["date", "hour", "host"])
    df = df.groupBy(df.date, df.hour, df.host).count()
    df = df.orderBy(df.date, df.hour, df.host)
    return df


def write_report(df, filename):
    """For the passed-in DataFrame, write out the contents to a CSV.
    """
    df = df.coalesce(1)
    temp_file = open(f"/out/{filename}", "w+")
    fieldnames = df.columns
    writer = csv.DictWriter(temp_file, fieldnames)
    writer.writerow(dict(zip(fieldnames, fieldnames)))

    for row in df.rdd.toLocalIterator():
        writer.writerow(row.asDict())


if __name__ == "__main__":
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--hours", action="store", type=int, required=True, help="Number of hours into the past to load Nginx logs")
    all_args.add_argument("--filename", action="store", type=str, required=True, help="Filename for the CSV report output")
    args = vars(all_args.parse_args())
    hours_ago = int(args["hours"])
    filename = str(args["filename"])
    session = spark_session(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    pyspark_handler = Log4JProxyHandler(session)
    LOGGER.addHandler(pyspark_handler)
    LOGGER.info("Starting report generation")
    df = read_nginx_logs(hours_ago, session, STORAGE_ACCOUNT_NAME)
    df = exclude_requests(df)
    df = process_dataframe(df)
    write_report(df, filename)
