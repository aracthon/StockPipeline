from kafka import KafkaProducer
from kfaka.errors import KafkaError
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import sys
import json
import time
import atexit
import logging

topic = ''
new_topic = ''
kafka_broker = ''
kafka_producer = ''

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('stream-processing')
logger.setLevel(logging.INFO)

def process(timeobj, rdd):
    num_of_records = rdd.count()
    if num_of_records == 0:
        return
    price_sum = rdd.map(
                    lambda record: float(json.loads(record[1].decode('utf-8'))[0].get('LastTradePrice'))
                ).reduce(
                    lambda a, b: a + b
                )
    average = price_sum / num_of_records
    logger.info('received %d records from kafka, average price is %f' % (num_of_records, average))

    data = json.dumps({
            'timestamp': time.time(),
            'average': average
    })

    kafka_producer.send(new_topic, value=data)

def shutdown_hook(producer):

if __name__ == '__main__':
    if len(sys.argv) != 4:
        print('Usage: stream-processing [topic] [new_topic] [kafka_broker]')
        exit(1)

    topic, new_topic, kafka_broker = sys.argv[1:]

    sc = SparkContext('local[2]', 'StockAveragePrice')
    sc.setLogLevel('ERROR')
    ssc = StreamingContext(sc, 5)

    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'retadata.broker.list': kafka_broker})
    directKafkaStream.foreachRDD(process)

    atexit.register(shutdown_hook, kafka_producer)

    ssc.start()
    ssc.awaitTermination()
