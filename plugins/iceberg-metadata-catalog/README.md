# iceberg-metadata-catalog

OpenSearch plugin that publishes OpenSearch index data to Apache Iceberg tables backed by AWS S3 Tables.

Exposes `POST /_iceberg/sync?index=<name>&role_arn=<arn>&s3_bucket=<name>&region=<region>` — broadcasts to all shards of the target index, discovers parquet files in the remote segment store, copies them into the S3 Tables warehouse, and registers them with the Iceberg REST catalog.

---

## AWS setup

You need **two AWS buckets** and **one IAM role**:

1. **S3 Tables bucket** — holds the Iceberg warehouse. Create via console or CLI:
   ```
   aws s3tables create-table-bucket --name <S3TABLES_BUCKET> --region <REGION>
   ```
   Note the returned ARN: `arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/<S3TABLES_BUCKET>`.

2. **Regular S3 bucket** for OpenSearch's remote segment store. This is where the composite engine writes parquet files before the plugin copies them.
   ```
   aws s3 mb s3://<REMOTE_STORE_BUCKET> --region <REGION>
   ```

3. **IAM role** in the S3 Tables account. The plugin `AssumeRole`s into this role at sync time.
   - **Permissions:** `s3tables:*` on the S3 Tables bucket ARN, `s3:*` on the warehouse's underlying S3 bucket.
   - **Trust policy:** allow the identity running OpenSearch (or the account containing it) to `sts:AssumeRole`. Example:
     ```
     {
       "Version": "2012-10-17",
       "Statement": [{
         "Effect": "Allow",
         "Principal": { "AWS": "arn:aws:iam::<CALLER_ACCOUNT_ID>:root" },
         "Action": "sts:AssumeRole"
       }]
     }
     ```

4. **Bootstrap credentials file** on the machine running OpenSearch. The plugin reads this to call `sts:AssumeRole`. Path is configurable via the `iceberg.credentials.file` setting.
   ```
   aws_access_key_id=...
   aws_secret_access_key=...
   aws_session_token=...
   ```
   The identity in this file must be the one allowed by the role's trust policy above.

5. **`~/.aws/credentials`** — separate from the bootstrap file above. `gradle/run.gradle` reads this to configure OpenSearch's S3 remote-store client, which needs access to `<REMOTE_STORE_BUCKET>` for reads/writes.

---

## Configure the run task

In `gradle/run.gradle`, replace the placeholder values:

- `<REGION>` — AWS region for both buckets
- `<ACCOUNT_ID>` — account holding the S3 Tables bucket
- `<S3TABLES_BUCKET>` — S3 Tables bucket name from step 1
- `<REMOTE_STORE_BUCKET>` — remote-store S3 bucket name from step 2
- `<PATH_TO_ICEBERG_BOOTSTRAP_CREDENTIALS>` — absolute path to the file from step 4

---

## Prerequisites for building

- JDK 25
- Rust toolchain (`cargo` on PATH)

---

## Build and run

Build the Rust native library:

```
./gradlew -Dsandbox.enabled=true :sandbox:libs:dataformat-native:buildRustLibrary
```

Run OpenSearch with the iceberg plugin:

```
./gradlew -Dsandbox.enabled=true run \
  -PrustDebug=true \
  -PinstalledPlugins="['repository-s3','arrow-base','arrow-flight-rpc','composite-engine','analytics-engine','parquet-data-format','analytics-backend-datafusion','analytics-backend-lucene','dsl-query-executor','iceberg-metadata-catalog']" \
  -x :distribution:docker:buildDockerImage \
  -x :distribution:docker:buildArm64DockerImage
```

---

## End-to-end sync flow

Once the cluster is running, walk through these steps in another shell.

### 1. Create a composite-engine index

```
curl -X PUT "http://localhost:9200/my-iceberg-test" \
  -H 'Content-Type: application/json' \
  -d '{
    "settings":{
      "index.number_of_shards":1,
      "index.number_of_replicas":0,
      "index.replication.type":"SEGMENT",
      "index.pluggable.dataformat.enabled":true,
      "index.pluggable.dataformat":"composite",
      "index.composite.primary_data_format":"parquet",
      "index.composite.secondary_data_formats":["lucene"]
    },
    "mappings":{
      "properties":{
        "host":{"type":"keyword"},
        "region":{"type":"keyword"}
      }
    }
  }'
```

### 2. Ingest documents

```
for i in $(seq 1 10); do
  curl -s -X POST "http://localhost:9200/my-iceberg-test/_doc" \
    -H 'Content-Type: application/json' \
    -d "{\"host\":\"h$((i%3))\",\"region\":\"<REGION>\"}" > /dev/null
done
```

### 3. Refresh, flush, and force-merge

Refresh makes docs searchable; flush + force-merge finalize the segments as parquet in the remote store.

```
curl -X POST "http://localhost:9200/my-iceberg-test/_refresh"
curl -X POST "http://localhost:9200/my-iceberg-test/_flush"
curl -X POST "http://localhost:9200/my-iceberg-test/_forcemerge?max_num_segments=1"
```

### 4. (Optional) Verify parquet is in the remote store

```
aws s3 ls s3://<REMOTE_STORE_BUCKET>/ --recursive | grep parquet
```

### 5. Trigger the sync

```
curl -X POST "http://localhost:9200/_iceberg/sync?index=my-iceberg-test&role_arn=arn:aws:iam::<ACCOUNT_ID>:role/<ROLE>&s3_bucket=<S3TABLES_BUCKET>&region=<REGION>"
```

### 6. Verify the Iceberg table exists

```
aws s3tables list-tables \
  --table-bucket-arn arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/<S3TABLES_BUCKET> \
  --namespace opensearch --region <REGION>

aws s3tables get-table \
  --table-bucket-arn arn:aws:s3tables:<REGION>:<ACCOUNT_ID>:bucket/<S3TABLES_BUCKET> \
  --namespace opensearch --name my-iceberg-test --region <REGION>
```

At this point the table is queryable from any Iceberg-aware engine (Athena, Trino, DuckDB, Spark, etc.).
