# - need to read to kafka, topic
# - need to write to cassandra, table

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
	helper function 
	@param stock_data
	@param cassandra_session, a session created using cassandra-driver
	"""

	try:
		logger.debug('Start to persist data to cassandra %s', stock_data)
		parsed = json.loads(stock_data)[0]
		symbol = parsed.get('StockSymbol')
		price = float(parsed.get('LastTradePrice'))
		tradetime = parsed.get('LastTradeDateTime')
		statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (data_table, symbol, tradetime, price)
		cassandra_session.execute(statement)
		logger.info('Persistend data to cassandra for symbol: %s, price: %f, tradetime: %s' % (symbol, price, tradetime))
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
	# - setup commandline arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('topic_name', help='the kafka topic')
	parser.add_argument('kafka_broker', help='the location of kafka broker')
	parser.add_argument('keyspace', help='keyspace to be used in cassandra')
	parser.add_argument('data_table', help='data table to be used')
	parser.add_argument('cassandra_broker', help='the location of cassandra cluster')

	# - parse arguments
	args = parser.parse_args()
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker
	keyspace = args.keyspace
	data_table = args.data_table
	cassandra_broker = args.cassandra_broker

	# - setup a kafka Consumer
	consumer = KafkaConsumer(
		topic_name, 
		bootstrap_servers=kafka_broker
	)

	# - setup a Cassandra Session
	cassandra_cluster = Cluster(
		contact_points=cassandra_broker.split(',')
	)
	session = cassandra_cluster.connect(keyspace)

	atexit.register(shutdown_hook, consumer, session)

	for msg in consumer:
		# - implement a function to sace data to cassandra
		persist_data(msg.value, session)



















