import argparse
import csv
import logging
import os
import sys

from utils import Log4JProxyHandler, spark_session, read_nginx_logs


STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("HttpRequestsSingleHost")


def exclude_requests(df):
    # Exclude requests from Nessus
    df = df.filter(df.remote_ip != "10.6.20.97")
    # Exclude requests from PRTG
    df = df.filter(df.user_agent != "Mozilla/5.0 (compatible; PRTG Network Monitor (www.paessler.com); Windows)")
    return df


def filter_requests(df, host):
    """Apply filters to the DataFrame to only include requests to host of interest.
    """
    df = df.filter(df.host == host)
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
    session = spark_session(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    session.sparkContext.setLogLevel("INFO")  # ALL,DEBUG,ERROR,FATAL,TRACE,WARN,INFO,OFF
    pyspark_handler = Log4JProxyHandler(session)
    LOGGER.addHandler(pyspark_handler)
    LOGGER.info("Starting report generation")
    df = read_nginx_logs(hours_ago, session, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, hours_offset=2)
    df = exclude_requests(df)
    df = filter_requests(df, host)
    write_report(df, filename)
