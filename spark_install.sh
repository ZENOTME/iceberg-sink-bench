set -ex

ICEBERG_VERSION=1.3.1
SPARK_VERSION=3.4.1

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:$ICEBERG_VERSION,org.apache.hadoop:hadoop-aws:3.3.2"
PACKAGES="$PACKAGES,org.apache.spark:spark-connect_2.12:$SPARK_VERSION"

SPARK_FILE="spark-${SPARK_VERSION}-bin-hadoop3.tgz"

wget https://dlcdn.apache.org/spark/spark-3.4.1/$SPARK_FILE
tar -xzf $SPARK_FILE --no-same-owner
