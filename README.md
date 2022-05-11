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

Example Kubernetes Cronjob YAML definition:

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
            - "240"
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
            imagePullPolicy: Always
            name: oracle-financials-logins
            resources:
              limits:
                cpu: "1"
                memory: 4Gi
  schedule: 0 5 * * *
  successfulJobsHistoryLimit: 3
  suspend: false
```
