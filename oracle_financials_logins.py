from azure.storage.blob import ContainerClient, BlobSasPermissions, generate_blob_sas
import argparse
import csv
from datetime import date, datetime, timedelta
from email.message import EmailMessage
from log4j4py import Log4JProxyHandler
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
from smtplib import SMTP
import sys


STORAGE_ACCOUNT_NAME = os.environ['STORAGE_ACCOUNT_NAME']
STORAGE_ACCOUNT_KEY = os.environ['STORAGE_ACCOUNT_KEY']
SMTP_SERVER = os.environ['SMTP_SERVER']
EMAIL_REPORT_SENDER = os.environ['EMAIL_REPORT_SENDER']
EMAIL_REPORT_RECIPIENTS = os.environ['EMAIL_REPORT_RECIPIENTS'].split(',')

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
LOGGER = logging.getLogger("OracleFinancialsLogins")


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


def oracle_financials_logs(df):
    """Apply filters to the DataFrame to only include requests to the Oracle Financials login path.
    """
    df = df.filter((df.host == "oraclefinancials.dbca.wa.gov.au") & (df.path == "/OA_HTML/AppsLogin"))
    df = df.orderBy(df.timestamp)

    return df


def upload_report_blob(df, hours_ago, start_date, blob_container="analytics"):
    """For the passed-in DataFrame, write out the contents to a CSV and upload to blob storage.
    """
    df = df.coalesce(1)
    temp_file = open("/tmp/placeholder.csv", "a+")

    fieldnames = df.columns
    writer = csv.DictWriter(temp_file, fieldnames)
    writer.writerow(dict(zip(fieldnames, fieldnames)))
    for row in df.toLocalIterator():
        writer.writerow(row.asDict())

    temp_file.seek(0)

    # Write the output to CSV in a defined container.
    day_count = int(hours_ago / 24)
    ds = start_date.strftime("%Y-%m-%d")
    blob_name = f"oracle_financials_logins/oracle_financials_logins_{day_count}_day_{ds}.csv"
    container_client = ContainerClient(
        account_url=f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net",
        container_name=blob_container,
        credential=STORAGE_ACCOUNT_KEY,
    )
    container_client.upload_blob(name=blob_name, data=temp_file.read(), overwrite=True)

    return blob_name


def send_email_report(blob, container="analytics"):
    """For a passed-in blob, generate a SAS token and email recipients a link to the blob.
    """
    sas_token = generate_blob_sas(
        account_name=STORAGE_ACCOUNT_NAME,
        container_name=container,
        blob_name=blob,
        account_key=STORAGE_ACCOUNT_KEY,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=365 * 3),  # Three years.
        start=datetime.utcnow(),
    )
    blob_url = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{container}/{blob}?{sas_token}"
    recipients = EMAIL_REPORT_RECIPIENTS
    smtp = SMTP(SMTP_SERVER)

    for recipient in recipients:
        # Construct the email.
        email = EmailMessage()
        email["Subject"] = "Oracle Financials access logs report"
        email["From"] = EMAIL_REPORT_SENDER
        email["To"] = recipient
        email.set_content(f'See attachment <a href="{blob_url}">{blob}</a>', subtype='html')
        # Send the message via Postmark SMTP server.
        smtp.send_message(email)
        LOGGER.info(f"Sending email to {recipient}")


if __name__ == "__main__":
    all_args = argparse.ArgumentParser()
    all_args.add_argument("--hours", action="store", type=int, required=True, help="Number of hours into the past to load Nginx logs")
    args = vars(all_args.parse_args())
    hours_ago = int(args["hours"])
    session = spark_session()
    pyspark_handler = Log4JProxyHandler(session)
    LOGGER.addHandler(pyspark_handler)
    LOGGER.info("Starting report generation")
    df = read_nginx_logs(hours_ago, session)
    df = oracle_financials_logs(df)
    blob_name = upload_report_blob(df, hours_ago, date.today())
    send_email_report(blob_name)
