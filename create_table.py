from pyspark.sql import SparkSession
import os
import psycopg2
from kafka.admin import KafkaAdminClient, NewTopic

g_spark = None

url = 'sc://localhost:15002'

access_key = os.environ.get('AWS_ACCESS_KEY')
secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

def get_sparks():
    global g_spark
    if g_spark is None:
        g_spark = SparkSession.builder.remote(url).getOrCreate()

    return g_spark


def init_iceberg_table(init_sqls):
    spark = get_sparks()
    for sql in init_sqls:
        print(f"Executing sql: {sql}")
        spark.sql(sql)

def execute(init_sqls):
    conn = psycopg2.connect(database="dev", user="root", password="", host="localhost", port="4566")

    cur = conn.cursor()

    for sql in init_sqls:
        print(f'Executing sql for rw: {sql}')
        cur.execute(sql)
        conn.commit()

    cur.close()
    conn.close()

if __name__ == "__main__":
    fields = ','.join([f'field{j} float' for j in range(1, 100)])
    sql1 = 'create schema if not exists db'
    sql = f'create table db.t (id int,{fields}) using iceberg tblproperties (\'format-version\'=\'2\',\'write.delete.mode\'=\'merge-on-read\');'
    init_sqls = [sql1,sql]
    init_iceberg_table(init_sqls)
    
    # create table in rw 
    fields = ','.join([f'field{j} real' for j in range(1, 100)])
    sql1 = f'create table t (id int primary key,{fields}) with (connector = \'kafka\',properties.bootstrap.server = \'localhost:29092\',topic = \'test\',scan.startup.mode=\'earliest\') FORMAT UPSERT ENCODE JSON;'
    sql = f'create sink s1 from t with (connector = \'iceberg\',type = \'upsert\',force_append_only = \'false\', database.name = \'db\',table.name = \'t\',catalog.type = \'storage\',warehouse.path = \'s3a://zj-iceberg-bench/demo/db\',s3.region = \'ap-east-1\',s3.access.key = \'{access_key}\',s3.secret.key = \'{secret_key}\',primary_key = \'id\')'
    init_sqls = [sql1,sql]
    execute(init_sqls)
