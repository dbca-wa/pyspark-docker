# PySpark inside Docker containers

Example run command:

```
docker container run --env STORAGE_ACCOUNT_NAME=storage_acct_name --env STORAGE_ACCOUNT_KEY=foobar \
--env SMTP_SERVER=smtp.server --env EMAIL_REPORT_SENDER=sender@email.com --env EMAIL_REPORT_RECIPIENTS=abe@email.com,bob@email.com \
pyspark-test driver local:///opt/application/oracle_financials_logins.py --hours 24
```

References:

* https://spot.io/blog/optimized-spark-docker-images-are-now-available/
* https://docs.datamechanics.co/docs/docker-images
