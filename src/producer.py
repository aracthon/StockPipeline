from googlefinance import getQuotes
from kafka import KafkaProducer
from kefka.errors import KafkaError, KafkaTimeoutError

import argparse
import atexit
import logging
import json
import schedule
import time

# - default kafka topic to write to
topic_name = 'stock-analyzer'

# - default kafka broker location
kafka_broker = '127.0.0.1:9092'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')

# - trace debug infor warning error
logger.setLevel(logging.DEBUG)

def fetch_price(producer, symbol):
    """
    helper function to get stock data and send to kafka
    @param producer - instance of a kafka producer
    @param symbol - symbol of stock, string type
    """

    logger.debug('Start to fetch stock price for %s', symbol)

    try:
        price = json.dumps(getQuotes(symbol))
        logger.debug('Get stock info %s', price)
        producer.send(topic_name, value=price, timestamp_ms=time.time())
        logger.debug('Sent stock price for %s to kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warn('Failed to send stock price for %s to kafka, caused by: %s',
                    (symbol, timeout_error))
    except Exception:
        logger.warn('Failed to get stock price for %s', symbol)

def shutdown_hook(producer):
    try:
        producer.flush(10)
        logger.info('Finished flushing pending message')
    except KafkaError as e:
        logger.warn('Failed to flush pending messages to kafka')
    finally:
        try:
            producer.close()
            logger.info('Kafka connection closed')
        except Exception as e:
            logger.warn('Failed to close kafka connection')

if __name__ == '__main__':

    # - CLI parser
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help='symbol of stock')
    parser.add_argument('topic_name', help='kafka topic')
    parser.add_argument('kafka_broker', help='the location of kafka broker')

    # - parse args
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = ars.topic_name
    kafka_broker = args.kafka_broker

    # - initiate a kafka producer
    producer = KafkaProducer(
            bootstrap_servers=kafka_broker
    )

    # - schedule to run every 1 sec
    schedule.every(1).second.do(fetch_price, producer, symbol)

    # - setup shutdown hook
    atexit.register(shutdown_hook, producer)

    while True:
        schedule.run_pending()
        time.sleep(1)
