set -ex

ICEBERG_VERSION=1.3.1
SPARK_VERSION=3.4.1

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

./spark-3.4.1-bin-hadoop3/sbin/start-connect-server.sh --packages $PACKAGES \
  --master local[3] \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.type=hadoop \
  --conf spark.sql.catalog.demo.warehouse=s3a://$AWS_S3_BUCKET/demo \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY \
  --conf spark.sql.catalog.demo.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.sql.defaultCatalog=demo

echo "Waiting spark connector server to launch on 15002..."

while ! nc -z localhost 15002; do
  sleep 1 # wait for 1/10 of the second before check again
done

echo "Spark connect server launched"
