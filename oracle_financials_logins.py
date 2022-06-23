from azure.storage.blob import ContainerClient, BlobSasPermissions, generate_blob_sas
import argparse
import csv
from datetime import date, datetime, timedelta
from email.message import EmailMessage
import logging
import os
from smtplib import SMTP
import sys

from utils import Log4JProxyHandler, spark_session, read_nginx_logs


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

    # Write the output to CSV in a defined blob container.
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
    session = spark_session(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    pyspark_handler = Log4JProxyHandler(session)
    LOGGER.addHandler(pyspark_handler)
    LOGGER.info("Starting report generation")
    df = read_nginx_logs(hours_ago, session, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
    df = oracle_financials_logs(df)
    blob_name = upload_report_blob(df, hours_ago, date.today())
    send_email_report(blob_name)
