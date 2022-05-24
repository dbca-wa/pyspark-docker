# PySpark inside Docker containers

A Dockerfile to run PySpark scripts which can to report HTTP requests to DBCA hosts.
Request logs are uploaded to Azure blob storage, so an Azure Storage Account name and
access key are required for use.

References:

* https://spot.io/blog/optimized-spark-docker-images-are-now-available/
* https://spot.io/blog/tutorial-running-pyspark-inside-docker-containers/
* https://docs.datamechanics.co/docs/docker-images

# PySpark shell session

Run the following command to get a Python shell session in a container:

```
docker container run -it ghcr.io/dbca-wa/pyspark-docker /opt/spark/bin/pyspark
```

# HTTP requests for a single host

A PySpark script to aggregate all requests to a single host for a defined number
of hours into the past. Suitable to run locally, the script will write the
report to CSV in a mounted volume.

To run the script locally, create a directory called `output`, run `chmod -R 777
output` to allow write access to it, then adapt the following command:

```
docker container run -v `pwd`/output:/out --env STORAGE_ACCOUNT_NAME="storage_acct_name" --env STORAGE_ACCOUNT_KEY="foobar" ghcr.io/dbca-wa/pyspark-docker driver local:///opt/application/http_requests_single_host.py --hours 24 --host=prs.dbca.wa.gov.au --filename=prs_requests.csv
```

# Oracle Financials login requests

A customised script to output reports of requests made to the Oracle Financials
login URL, for audit purposes. The script generates a report, uploads to Azure
blob storage, and emails a link to nominated email addresses. Suitable for
running locally or as a Kubernetes CronJob.

Example Docker run command:

```
docker container run --env STORAGE_ACCOUNT_NAME=storage_acct_name --env STORAGE_ACCOUNT_KEY=foobar \
--env SMTP_SERVER=smtp.server --env EMAIL_REPORT_SENDER=sender@email.com --env EMAIL_REPORT_RECIPIENTS=abe@email.com,bob@email.com \
ghcr.io/dbca-wa/pyspark-docker driver local:///opt/application/oracle_financials_logins.py --hours 24
```

Example Kubernetes CronJob YAML definition:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: oracle-financials-logins
  namespace: pyspark
spec:
  concurrencyPolicy: Forbid
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - args:
            - /opt/spark/bin/spark-submit
            - --conf
            - spark.driver.bindAddress=
            - --deploy-mode
            - client
            - local:///opt/application/oracle_financials_logins.py
            - --hours
            - "24"
            env:
            - name: SMTP_SERVER
              value: smtp.lan.fyi
            - name: EMAIL_REPORT_SENDER
              value: noreply@dbca.wa.gov.au
            - name: EMAIL_REPORT_RECIPIENTS
              value: joe.bloggs@dbca.wa.gov.au
            - name: TZ
              value: Australia/Perth
            envFrom:
            - secretRef:
                name: azure-storage-account
                optional: false
            image: ghcr.io/dbca-wa/pyspark-docker
            name: oracle-financials-logins
            resources:
              limits:
                cpu: "1"
                memory: 4Gi
  schedule: 0 5 * * *
  successfulJobsHistoryLimit: 3
  suspend: false
```
