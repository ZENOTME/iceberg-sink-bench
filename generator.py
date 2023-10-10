from kafka import KafkaProducer
import time
import argparse

# Kafka集群的地址
bootstrap_servers = 'localhost:29092'
topic = 'test'
field_start = 1
field_end = 100
# sec
interval = 0.1

# 创建一个KafkaProducer实例
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def add(i):
    print("add", i)
    key = f'{{"id":{i}}}'
    fields = ','.join([f'"field{j}":{i}' for j in range(field_start, field_end)])
    value = f'{{"id":{i},{fields}}}'
    producer.send(topic, key=key.encode('utf-8'), value=value.encode('utf-8'))

def delete(i):
    print("delete", i)
    key = f'{{"id":{i}}}'
    value = f''
    producer.send(topic, key=key.encode('utf-8'), value=value.encode('utf-8'))


# 创建 ArgumentParser 对象
parser = argparse.ArgumentParser()

# 添加命令行参数
parser.add_argument('-b', type=int, help='begin')
parser.add_argument('-e', type=int, help='end')

# 解析命令行参数
args = parser.parse_args()

# 获取命令行参数的值
start_value = args.b
end_value = args.e

# 输出命令行参数的值
print('begin:', start_value)
print('end:', end_value)

i = start_value

# 每插入100条数据，删除前50条数据
while True:
    add(i)
    # delete every 100 write
    if i != 0 and i % 100 == 0:
        for j in range(50):
            delete(i - 100 + j)
    i += 1
    if i == end_value:
        break
    # time.sleep(interval)

# 关闭生产者
producer.close()
