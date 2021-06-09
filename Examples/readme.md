# Playing with MongoDB data in a Jupyter notebook

To use MongoDB data with Spark create a new Python Jupyter notebook by navigating to the Jupyter URL and under notebook select Python 3 :

![Image of New Python notebook](https://github.com/fredfav/mongo-spark/images/newpythonnotebook.png)

Now you can run through the following demo script.  You can copy and execute one or more of these lines :

To start, this will create the SparkSession and set the environment to use our local MongoDB cluster.

```
from pyspark.sql import SparkSession

spark = SparkSession.\
        builder.\
        appName("pyspark-spark-example").\
        master("spark://spark-master:7077").\
        config("spark.executor.memory", "1g").\
        config("spark.mongodb.input.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0").\
        config("spark.mongodb.output.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/Stocks.Source?replicaSet=rs0").\
        config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
        getOrCreate()
```
Next load the dataframes from MongoDB
```
df = spark.read.format("mongo").load()
```
Let’s verify the data was loaded by looking at the schema:
```
df.printSchema()
```
We can see that the tx_time field is loaded as a string.  We can easily convert this to a time by issuing a cast statement:

`df = df.withColumn(‘tx_time”, df.tx_time.cast(‘timestamp’))`

Next, we can add a new ‘movingAverage’ column that will show a moving average based upon the previous value in the dataset.  To do this we leverage the PySpark Window function as follows:

```from pyspark.sql.window import Window
from pyspark.sql import functions as F

movAvg = df.withColumn("movingAverage", F.avg("price")
             .over( Window.partitionBy("company_symbol").rowsBetween(-1,1)) )
```
To see our data with the new moving average column we can issue a 
movAvg.show().

`movAvg.show()`

To update the data in our MongoDB cluster, we  use the save method.

`movAvg.write.format("mongo").option("replaceDocument", "true").mode("append").save()`

We can also use the power of the MongoDB Aggregation Framework to pre-filter, sort or aggregate our MongoDB data.

```
pipeline = "[{'$group': {_id:'$company_name', 'maxprice': {$max:'$price'}}},{$sort:{'maxprice':-1}}]"
aggPipelineDF = spark.read.format("mongo").option("pipeline", pipeline).option("partitioner", "MongoSinglePartitioner").load()
aggPipelineDF.show()
```

Finally we can use SparkSQL to issue ANSI-compliant SQL against MongoDB data as follows:

```
movAvg.createOrReplaceTempView("avgs")
sqlDF=spark.sql("SELECT * FROM avgs WHERE movingAverage > 43.0")
sqlDF.show()
```

In this repository we created a JupyterLab notebook, leaded MongoDB data, computed a moving average and updated the collection with the new data.  This simple example shows how easy it is to integrate MongoDB data within your Spark data science application.  For more information on the Spark Connector check out the [online documentation](https://docs.mongodb.com/spark-connector/master/).  For anyone looking for answers to questions feel free to ask them in the [MongoDB community pages](https://developer.mongodb.com/community/forums/c/connectors-integrations/48).  The MongoDB Connector for Spark is [open source](https://github.com/mongodb/mongo-spark) under the Apache license.  Comments/pull requests are encouraged and welcomed.  Happy data exploration!

# Examples
* spark-example: loading data, ETL and SQL query
* spark-ml-example: loading data, ETL and Machine Learning