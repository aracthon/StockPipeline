echo "Starting docker-machine bigdata"

docker-machine start bigdata

echo "Connecting bigdata server"

eval $(docker-machine env bigdata)

echo "Starting docker container"

docker start zookeeper
docker start kafka
docker start cassandra

echo "Activating Virtualenv with Python 2.7.10"

source ~/Desktop/bigdata/stock/env/bin/activate

echo "Complete!"
