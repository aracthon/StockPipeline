
echo "Deactivating Virtualenv with Python 2.7.10"

deactivate

echo "Stoping docker container"

docker stop zookeeper
docker stop kafka
docker stop cassandra

echo "Stoping docker-machine bigdata"

docker-machine stop bigdata

echo "Complete!"