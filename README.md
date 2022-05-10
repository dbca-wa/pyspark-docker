# PySpark inside Docker containers

Example run command:

```
docker container run --env STORAGE_ACCOUNT_NAME="storage_acct_name" --env STORAGE_ACCOUNT_KEY="foobar" --env SMTP_SERVER="smtp.server" --env SMTP_PASSWORD="smtppass" pyspark-test driver local:///opt/application/oracle_financials_logins.py --hours 12
```
