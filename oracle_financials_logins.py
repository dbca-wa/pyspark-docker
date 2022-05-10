import argparse
from datetime import date, datetime, timedelta
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType


STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']


def spark_session():
    """Get and return a Spark session.
    """
    session = SparkSession.builder.getOrCreate()
    session.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT_NAME}.blob.core.windows.net", STORAGE_ACCOUNT_KEY)
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


def oracle_financials_logs(df):
    # Only include requests to the Oracle Financials login path.
    df = df.filter((df.host == "oraclefinancials.dbca.wa.gov.au") & (df.path == "/OA_HTML/AppsLogin"))
    df = df.orderBy(df.timestamp)
    return df


def write_report_blob(df, hours_ago, start_date, blob_container="analytics"):
    # Write the output to CSV in a defined container.
    day_count = int(hours_ago / 24)
    ds = start_date.strftime("%Y-%m-%d")
    out_filename = f"oracle_financials_logins/oracle_financials_logins_{day_count}_day_{ds}"
    file_path = f"wasbs://{blob_container}@{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{out_filename}.csv"
    df.coalesce(1).write.mode("overwrite").csv(file_path, sep=",", header=True)


if __name__ == "__main__":
    # Construct an argument parser
    all_args = argparse.ArgumentParser()
    # Add arguments to the parser
    all_args.add_argument("--hours", action="store", type=int, required=True, help="Number of hours into the past to load Nginx logs")
    args = vars(all_args.parse_args())

    hours_ago = int(args["hours"])
    session = spark_session()
    df = read_nginx_logs(hours_ago, session)
    df = oracle_financials_logs(df)
    write_report_blob(df, hours_ago, date.today())
