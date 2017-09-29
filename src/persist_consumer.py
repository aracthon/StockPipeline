from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster


import logging
import argparse
import json
import atexit

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')

# - TRACE DEBUG INFO WARNING ERROR
logger.setLevel(logging.DEBUG)

# - default kafka setting
topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9092'

# - default cassandra setting
keyspace = 'stock'
data_table = ''
cassandra_broker = '127.0.0.1:9042'

def persist_data(stock_data, cassandra_session):
    """
    helper function to persist data from kafka to cassandra
    @param stock_data - a json object
    @param cassandra_session - a session instance
    """

    try:
        logger.debug('Start to persist data to cassandra %s', stock_data)
        parsed = json.loads(stock_data)[0]
        symbol = parsed.get('StockSymbol')
        price = float(parsed.get('LastTradePrice'))
        trade_time = parsed.get('LastTradeTime')

        statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) \
                            VALUES ('%s', '%s', '%f')" % (data_table, symbol, tradetime, price)
        cassandra_session.execute(statement)
        logger.info('Persisted data to cassandra for \
                    symbol: %s, price: %s, tradetime: %s' % (symbol, tradetime, price)))
    except Exception as e:
        logger.error('Failed to persist data to cassandra %s', stock_data)

def shutdown_hook(consumer, session):
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
	except KafkaError as kafka_error:
		logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
	finally:
		logger.info('Existing program')

if __name__ == '__main__':

    # - CLI parser
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name')
    parser.add_argument('kafka_broker')
    parser.add_argument('keyspace')
    parser.add_argument('data_table')
    parser.add_argument('cassandra_broker')

    # - parse args
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	keyspace = args.keyspace
	data_table = args.data_table
	cassandra_broker = args.cassandra_broker

    # - setup a kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )

    # - setup a cassandra session
    cassandra_cluster = Cluster(
        contact_points=cassandra_broker.split(',')
    )
    session = cassandra_cluster.connect(keyspace)

    atexit.register(shutdown_hook, consumer, session)

    for msg in consumer:
        persist_data(msg.value, session)
