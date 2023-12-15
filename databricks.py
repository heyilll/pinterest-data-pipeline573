# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
import urllib

# Specify file type to be csv
file_type = "csv"
# Indicates file has first row as the header
first_row_is_header = "true"
# Indicates file has comma as the delimeter
delimiter = ","
# Read the CSV file to spark dataframe
aws_keys_df = spark.read.format(file_type)\
.option("header", first_row_is_header)\
.option("sep", delimiter)\
.load("/FileStore/tables/authentication_credentials.csv")

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.where(col('User name')=='databricks-user').select('Secret access key').collect()[0]['Secret access key']
# Encode the secret key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

pindf = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e90e0175553-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()
 

# COMMAND ----------

pindf = pindf.selectExpr("CAST(data as STRING) AS jsonData") 
# display(pindf) 
schema = StructType([ 
                     StructField("index", StringType(), True), 
                     StructField("unique_id", StringType(), True), 
                     StructField("title", StringType(), True), 
                     StructField("description", StringType(), True), 
                     StructField("follower_count", StringType(), True),
                     StructField("poster_name", StringType(), True), 
                     StructField("tag_list", StringType(), True), 
                     StructField("is_image_or_video", StringType(), True), 
                     StructField("image_src", StringType(), True),
                     StructField("save_location", StringType(), True), 
                     StructField("category", StringType(), True), ])

pindf = pindf.select(from_json('jsonData',schema) \
     .alias("data")) \
     .select("data.*") 
display(pindf)

# COMMAND ----------

df_pin = pindf.select('*')

df_pin = df_pin.withColumnRenamed('index', 'ind') 
df_pin= df_pin.withColumn( 'save_location', regexp_replace('save_location', 'Local save in ', ''))
df_pin= df_pin.withColumn('follower_count', regexp_replace('follower_count', 'k', '000'))
df_pin= df_pin.withColumn('follower_count', regexp_replace('follower_count', 'M', '000000'))
df_pin= df_pin.withColumn('follower_count', col('follower_count').cast("int"))
df_pin= df_pin.withColumn('ind', col('ind').cast("int"))
df_pin = df_pin.select([when(col(c)=="No description available Story format",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="No description available",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="Untitled",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="No Title Data Available",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="User Info Error",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="N,o, ,T,a,g,s, ,A,v,a,i,l,a,b,l,e",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select([when(col(c)=="Image src error.",None).otherwise(col(c)).alias(c) for c in df_pin.columns])
df_pin = df_pin.select('ind', 'unique_id', 'title','description', 'follower_count', 'poster_name', 'tag_list', 'is_image_or_video', 'image_src', 'save_location',  'category')

display(df_pin)

# COMMAND ----------

udf = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e90e0175553-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

display(udf)

# COMMAND ----------

uudf = udf.selectExpr("CAST(data as STRING) AS jsonData")

schema = StructType([ 
                     StructField("ind", StringType(), True), 
                     StructField("first_name", StringType(), True), 
                     StructField("last_name", StringType(), True), 
                     StructField("age", StringType(), True), 
                     StructField("date_joined", StringType(), True) ])

uudf = uudf.select(from_json('jsonData',schema) \
     .alias("data")) \
     .select("data.*") 

display(uudf)     

# COMMAND ----------

df_user = uudf.select('*')
  
df_user= df_user.withColumn( 'user_name', concat( 'first_name', lit(' '),'last_name'))
df_user = df_user.drop('first_name') 
df_user = df_user.drop('last_name') 
df_user= df_user.withColumn('date_joined', col('date_joined').cast("timestamp"))
df_user= df_user.withColumn('age', col('age').cast("int"))
df_user = df_user.select('ind', 'user_name', 'age','date_joined')
display(df_user)

# COMMAND ----------

gdf = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0e90e0175553-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()



# COMMAND ----------

gdf = gdf.selectExpr("CAST(data as STRING) AS jsonData") 
 
schema = StructType([ 
                     StructField("ind", StringType(), True), 
                     StructField("timestamp", StringType(), True), 
                     StructField("latitude", StringType(), True), 
                     StructField("longitude", StringType(), True), 
                     StructField("country", StringType(), True) ])

gdf = gdf.select(from_json('jsonData',schema) \
     .alias("data")) \
     .select("data.*") 

display(gdf)     

# COMMAND ----------

df_geo = gdf.select('*')
  
df_geo= df_geo.withColumn('coordinates', struct(df_geo.latitude, df_geo.longitude))
df_geo = df_geo.drop('latitude') 
df_geo = df_geo.drop('longitude') 
df_geo= df_geo.withColumn('timestamp', col('timestamp').cast("timestamp"))
df_geo = df_geo.select('ind', 'country', 'coordinates','timestamp')
display(df_geo)

# COMMAND ----------

df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e90e0175553_pin_table")

# COMMAND ----------

df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e90e0175553_user_table")

# COMMAND ----------

df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0e90e0175553_geo_table")
