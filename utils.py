from datetime import datetime, timedelta
import logging
from logging import Handler, LogRecord
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, IntegerType
from typing import Any


class Log4JProxyHandler(Handler):
    """Handler to forward messages to log4j.
    """

    Logger: Any

    def __init__(self, spark_session: SparkSession):
        """Initialise handler with a log4j logger."""
        Handler.__init__(self)
        self.Logger = spark_session._jvm.org.apache.log4j.Logger

    def emit(self, record: LogRecord):
        """Emit a log message."""
        logger = self.Logger.getLogger(record.name)
        if record.levelno >= logging.CRITICAL:
            # Fatal and critical seem about the same.
            logger.fatal(record.getMessage())
        elif record.levelno >= logging.ERROR:
            logger.error(record.getMessage())
        elif record.levelno >= logging.WARNING:
            logger.warn(record.getMessage())
        elif record.levelno >= logging.INFO:
            logger.info(record.getMessage())
        elif record.levelno >= logging.DEBUG:
            logger.debug(record.getMessage())
        else:
            pass


def spark_session(storage_account_name, storage_account_key):
    """Get and return a Spark session.
    """
    session = SparkSession.builder.getOrCreate()
    session.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)
    session.conf.set("network.timeout", 1200)
    return session


def read_nginx_logs(hours_ago, session, storage_account_name, blob_container="access-logs", hours_offset=None):
    """Read Nginx logs from blob storage for a given number of hours into the past
    and parse them into a Spark session, returning a DataFrame.
    `hours_offset` is an optional integer which offsets backwards from the current time, in order to account
    for any expected delays in log shipping. Where this value is ommitted, the starting time is set at the
    beginning of the current date (i.e. 0:00:00 of the current date).
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
    if not hours_offset:
        t = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)  # Start at the beginning of the current date.
    else:
        t = datetime.now() - timedelta(hours=hours_offset)  # Start the set amount of hours ago.

    for i in range(hours_ago):
        csv_path = filename.format(blob_container, storage_account_name, t.strftime("%Y%m%d%H"))
        file_list.append(csv_path)
        t = t - timedelta(hours=1)

    df = session.read.options(mode="DROPMALFORMED").load(file_list, format='csv', schema=schema)
    df = df.withColumn("timestamp", df.timestamp[0:19])  # Slice off the timezone string.
    # Re-cast the timestamp column type.
    df = df.withColumn("timestamp", df.timestamp.cast(TimestampType()))

    return df
