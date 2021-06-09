# Import libraries
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator 
from pyspark.ml.recommendation import ALS
from pyspark.sql import functions
from pyspark.sql.types import DoubleType

# Open Spark session
spark = SparkSession.\
builder.\
appName("pyspark-notebook2").\
    master("spark://spark-master:7077").\
    config("spark.executor.memory", "1g").\
    config("spark.mongodb.input.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/recommendation.ratings?replicaSet=rs0").\
    config("spark.mongodb.output.uri","mongodb://mongo1:27017,mongo2:27018,mongo3:27019/recommendation.ratings?replicaSet=rs0").\
    config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0").\
    getOrCreate()

# Loading data into Spark
ratings_df = spark.read.format("mongo").load()

# Looking at the schema dataset
ratings_df.cache() 
ratings_df.printSchema()

# Perform a descriptive analysis
ratings_df.describe().toPandas().transpose()

# Splitting the dataset into 80/20 for training and test
splits = ratings_df.randomSplit([0.8,0.2]) 
train_df = splits[0]
test_df = splits[1]

# Build the recommendation model using ALS on the training data
als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").\
    setItemCol("movieId").setRatingCol("rating")
model = als.fit(train_df)

# Evaluate the model by computing the RMSE on the test data
# Setting cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
model.setColdStartStrategy("drop") 
predictions = model.transform(test_df)
evaluator = RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").\
    setPredictionCol("prediction")
rmse = evaluator.evaluate(predictions) 
print("Root-mean-square error = ", rmse)

# Display predictions
predictions.show()

# Using the model to generate a set of 10 recommended movies for each user in the dataset
columns = ["_id", "timestamp"] 
docs = predictions.drop(*columns)

# Need to cast predictions from FloatType to Double as Float is not a BSON type in MongoDB
docs = docs.withColumn("prediction", docs.prediction.cast(DoubleType())) 
docs.printSchema()

# Write recommendations to MongoDB
docs.write.format("mongo").mode("overwrite").option("database", "recommendation").option("collection", "recommendations").save()

# Read recommendations for a user
pipeline = "[{'$match': {'userId': 1}}, {'$project': {'_id': 0, 'timestamp': 0}}, {'$sort': {'rating': -1}}, {'$limit': 10}]"
aggPipelineDF = spark.read.format("mongo").option("pipeline", pipeline).option("partitioner", "MongoSinglePartitioner").load()
aggPipelineDF.show()