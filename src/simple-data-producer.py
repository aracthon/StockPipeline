# - AAPL, GOOG


from googlefinance import getQuotes
from kafka.errors import KafkaError, KafkaTimeoutError
from kafka import KafkaProducer

import argparse
import json
import time
import logging
import schedule
import atexit

# - default kafka setting
topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9092'

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')

# - TRACE DEBUG INFO WARNING ERROR
logger.setLevel(logging.DEBUG)

def fetch_price(producer, symbol):
	"""
	helper function to get stock data and send to kafka
	@param producer - instance of a kafka producer
	@param symbol   - symbol of stock, string type
	@return None
	"""
	logger.debug('Start to fetch stock proce for %s', symbol)
	try:
		price = json.dumps(getQuotes(symbol))
		logger.debug('Get stock info %s', price)
		# print (price)
		producer.send(topic_name, value=price, timestamp_ms=time.time())
		logger.debug('Sent stock price for %s to kafka', symbol)
	except KafkaTimeoutError as timeout_error	:
		logger.warn('Failed to send stock prce for %s to kafka, caused by: %s', (symbol, timeout_error))
		pass
	except Exception:
		logger.warn('Failed to get stock price for %s', symbol)

def shutdown_hook(producer):
	try:
		producer.flush(10)
		logger.info('Finished flushing pending messages')
	except KafkaError as KafkaError:
		logger.warn('Failed to flush pending messages to kafka')
	finally:
		try:
			producer.close()
			logger.info('Kafka connection closed')
		except Exception as e:
			logger.warn('Failed to close kafka connection')

if __name__ == '__main__':
	# - setup commandline arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('symbol', help='the symbol of the stock')
	parser.add_argument('topic_name', help='the kafka topic')
	parser.add_argument('kafka_broker', help='the location of kafka broker')

	# - parse arguments
	args = parser.parse_args()
	symbol = args.symbol
	topic_name = args.topic_name
	kafka_broker = args.kafka_broker

	# - initiate a kafka producer
	producer = KafkaProducer(
		bootstrap_servers = kafka_broker
	)

	# - schedule to run every 1 sec
	schedule.every(1).second.do(fetch_price, producer, symbol)

	# - setup proper shutdown hook
	atexit.register(shutdown_hook, producer)

	while True:
		schedule.run_pending()
		time.sleep(1)
