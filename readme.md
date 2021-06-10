# Using MongoDB with Jupyter Labs

This repository showcases how to leverage MongoDB data in your JupyterLab notebooks via the MongoDB Spark Connector and PySpark.  We will load financial security data from MongoDB, calculate a moving average then update the data in MongoDB with these new data.  This repository has two components:
- Docker files
- Data generator (optional, see end of this text)

The Docker files will spin up the following environment:

![Image of docker environment](https://github.com/Fredfav/mongo-spark-jupyter/blob/master/images/diagram.png)

## Getting the environment up and running

Execute the `run.sh` script file.  This runs the docker compose file which creates a three node MongoDB cluster, configures it as a replica set on prt 27017. Spark is also deployed in this environment with a master node located at port 8080 and two worker nodes listening on ports 8081 and 8082 respectively.  The MongoDB cluster will be used for both reading data into Spark and writing data from Spark back into MongoDB.

Note: You may have to mark the .SH file as runnable with the `chmod` command i.e. `chmod +x run.sh`

**If you are using Windows, launch a PowerShell command window and run the `run.ps1` script instead of run.sh.**

To verify our Spark master and works are online navigate to http://localhost:8080

The Jupyter notebook URL which includes its access token will be listed at the end of the script.  NOTE: This token will be generated when you run the docker image so it will be different for you.  Here is what it looks like:

![Image of url with token](https://github.com/Fredfav/mongo-spark-jupyter/blob/master/images/url.png)

If you launch the containers outside of the script, you can still get the URL by issuing the following command:

`docker exec -it jupyterlab  /opt/conda/bin/jupyter notebook list`

or

`docker exec -it jupyterlab  /opt/conda/bin/jupyter server list`



# Data Generator (optional)

This repository comes with a small sample data set already so it is not necessary to use this tool, however, if you are interested in creating a larger dataset with the same type of financial security data you run the Python3 app within the DataGenerator directory.

`python3 create-stock-data.py`

Parameter | Description
--------- | ------------
-s | number of financial stock symbols to generate, default is 10
-c | MongoDB Connection String, default is mongodb://localhost
-d | MongoDB Database name default is Stocks
-w | MongoDB collection to write to default is Source
-r | MongoDB collection to read from default is Sink

This data generator tool is designed to write to one collection and read from another.  It is also used as part of a Kafka connector demo where the data is flowing through the Kafka system.  In our repository example, if you just want to see the data as it is written to the "Source" collection use the -r parameter as follows:

`python3 create-stock-data.py -r Source`
