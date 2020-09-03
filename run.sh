#!/bin/bash

set -e
(
if lsof -Pi :27017 -sTCP:LISTEN -t >/dev/null ; then
    echo "Please terminate the local mongod on 27017"
    exit 1
fi
)

echo "Starting docker ."
docker-compose up -d --build

function clean_up {
    echo "\n\nShutting down....\n\n"
    
    docker-compose down -v
}

trap clean_up EXIT

echo -e "\nConfiguring the MongoDB ReplicaSet.\n"
docker-compose exec mongo1 /usr/bin/mongo --eval '''if (rs.status()["ok"] == 0) {
    rsconf = {
      _id : "rs0",
      members: [
        { _id : 0, host : "mongo1:27017", priority: 1.0 },
        { _id : 1, host : "mongo2:27017", priority: 0.5 },
        { _id : 2, host : "mongo3:27017", priority: 0.5 }
      ]
    };
    rs.initiate(rsconf);
}

rs.conf();

'''
echo -e "\nUploading test data into CDCData database\n"

docker-compose exec mongo1 apt-get update && apt-get install wget && wget https://raw.githubusercontent.com/RWaltersMA/mongo-spark-jupyter/master/data.csv
sleep 2
docker-compose exec mongo1 /usr/bin/mongoimport -d=CDCData -c=obesity --type=csv --file=data.csv --drop --headerline

echo '''



==============================================================================================================
MongoDB Spark Demo

Jypterlab - http://localhost:8888
Spark Master - http://localhost:8080
Spark Worker 1
Spark Worker 2
MongoDB Replica Set - port 27017-27019

==============================================================================================================

Use <ctrl>-c to quit'''

read -r -d '' _ </dev/tty
echo '\n\nTearing down the Docker environment, please wait.\n\n'

# if we don't specify -v then issue this one -> docker-compose exec mongo1 /usr/bin/mongo localhost:27017/SparkDemo --eval "db.dropDatabase()"

dockder-compose down  -v

# note: we use a -v to remove the volumes, else you'll end up with old data