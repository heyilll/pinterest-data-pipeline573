# Databricks notebook source
dbutils.fs.ls("/FileStore/tables")

# COMMAND ----------

# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib
import numpy as np 

# COMMAND ----------

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
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0e90e0175553-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/mount1"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
# dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount1/topics/0e90e0175553.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
gdf = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(gdf)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount1/topics/0e90e0175553.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
pindf = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(pindf)

# COMMAND ----------

# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/mount1/topics/0e90e0175553.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
udf = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(udf)

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

df_geo = gdf.select('*')
 
# df_geo= df_geo.withColumn( 'coordinates', zip_with("latitude", "longitude"))
df_geo= df_geo.withColumn('coordinates', struct(df_geo.latitude, df_geo.longitude))
df_geo = df_geo.drop('latitude') 
df_geo = df_geo.drop('longitude') 
df_geo= df_geo.withColumn('timestamp', col('timestamp').cast("timestamp"))
df_geo = df_geo.select('ind', 'country', 'coordinates','timestamp')
display(df_geo)

# COMMAND ----------

df_user = udf.select('*')
 
# df_user= df_user.withColumn( 'coordinates', zip_with("latitude", "longitude"))
df_user= df_user.withColumn( 'user_name', concat( 'first_name', lit(' '),'last_name'))
df_user = df_user.drop('first_name') 
df_user = df_user.drop('last_name') 
df_user= df_user.withColumn('date_joined', col('date_joined').cast("timestamp"))
df_user= df_user.withColumn('age', col('age').cast("int"))
df_user = df_user.select('ind', 'user_name', 'age','date_joined')
display(df_user)

# COMMAND ----------

df_geo.createOrReplaceTempView("df_geot")
df_pin.createOrReplaceTempView("df_pint")
df_user.createOrReplaceTempView("df_usert")

query = """
with data as 
(SELECT g.country as country, p.category as category, COUNT(category) as ttt, RANK() OVER (PARTITION BY country ORDER BY COUNT(category) DESC) rank
FROM df_geot as g
JOIN df_pint as p
ON g.ind == p.ind 
GROUP BY country, category
ORDER BY country, ttt DESC )

SELECT country, category
FROM data
WHERE rank = 1  
ORDER BY country  
""" 

result = spark.sql(query)
result.show()
 

# COMMAND ----------

query = """
SELECT YEAR(g.timestamp) as year, p.category, COUNT(p.category) as category_count
FROM df_geot as g
JOIN df_pint as p
ON g.ind == p.ind 
WHERE 2017 < YEAR(g.timestamp)
GROUP BY p.category, year
ORDER BY year, p.category DESC
""" 

result = spark.sql(query)
result.show()

# COMMAND ----------

query = """
with data as (
    SELECT g.country as country, g.ind as iind, p.poster_name as pn, p.follower_count as ttt, RANK() OVER (PARTITION BY country ORDER BY p.follower_count DESC) rank  
    FROM df_geot as g
    JOIN df_pint as p
    ON g.ind == p.ind 
    GROUP BY country, iind, pn, ttt
    ORDER BY country, ttt DESC
) 
SELECT country, pn, ttt as follower_count
FROM data 
WHERE rank = 1
GROUP BY country, pn, ttt  
ORDER BY follower_count DESC
""" 

result = spark.sql(query)
result.show() 

# COMMAND ----------

query = """
SELECT country, pn as postername, follower_count
FROM (SELECT country, pn, ttt as follower_count, ROW_NUMBER() OVER (ORDER BY ttt DESC) as ranked
    FROM (SELECT g.country as country, g.ind as iind, p.poster_name as pn, p.follower_count as ttt, RANK() OVER (PARTITION BY country ORDER BY p.follower_count DESC) rank  
        FROM df_geot as g
        JOIN df_pint as p
        ON g.ind == p.ind 
        GROUP BY country, iind, pn, ttt
        ORDER BY country, ttt DESC
    ) 
    WHERE rank = 1
    GROUP BY country, pn, ttt  
    ORDER BY follower_count DESC)  
WHERE ranked = 1
""" 

result = spark.sql(query)
result.show() 

# COMMAND ----------

query = """ 
with data as (
SELECT CASE WHEN u.age <= 25 THEN '18-24' 
            WHEN u.age <= 35 THEN '25-35'  
            WHEN u.age <= 50 THEN '35-50' 
            WHEN u.age > 50 THEN '50+'
        END as age, p.category as category   	
FROM df_usert as u   
JOIN df_pint as p
ON u.ind == p.ind  
)

SELECT age, category, ttt as category_count
  FROM(
    SELECT age, category, COUNT(category) as ttt, RANK() OVER (PARTITION BY age ORDER BY COUNT(data.category) DESC) rank 	
    FROM data    
    GROUP BY age, category 
    ORDER BY ttt DESC 
) 
WHERE rank = 1
""" 

result = spark.sql(query)
result.show()

# COMMAND ----------

query = """
SELECT age, 
    percentile_approx(follower_count, 0.5) as median_follower_count  
FROM  (SELECT CASE WHEN u.age <= 25 THEN '18-24' 
            WHEN u.age <= 35 THEN '25-35'  
            WHEN u.age <= 50 THEN '35-50' 
            WHEN u.age > 50 THEN '50+'
        END as age, p.follower_count as follower_count   	
FROM df_usert as u   
JOIN df_pint as p
ON u.ind == p.ind)    
GROUP BY age 
ORDER BY median_follower_count DESC
""" 

result = spark.sql(query) 
result.show(result.count())

# COMMAND ----------

query = """
SELECT year, 
    COUNT(year) as number_users_joined
FROM (
    SELECT YEAR(u.date_joined) as year
    FROM df_usert as u 
    WHERE YEAR(u.date_joined) > 2014 AND YEAR(u.date_joined) < 2021
)
GROUP BY year
ORDER BY year DESC
""" 

result = spark.sql(query) 
result.show(result.count())

# COMMAND ----------

query = """
SELECT year as post_year, 
    percentile_approx(follower_count, 0.5) as median_follower_count
FROM (
    SELECT YEAR(u.date_joined) as year, p.follower_count
    FROM df_usert as u
    JOIN df_pint as p
    ON u.ind == p.ind 
    WHERE YEAR(u.date_joined) > 2014 AND YEAR(u.date_joined) < 2021
)
GROUP BY post_year
ORDER BY post_year DESC
""" 

result = spark.sql(query) 
result.show(result.count())

# COMMAND ----------

query = """
SELECT age, year, 
    percentile_approx(follower_count, 0.5) as median_follower_count
FROM (
    SELECT YEAR(u.date_joined) as year, p.follower_count, CASE WHEN u.age <= 25 THEN '18-24' 
                                                            WHEN u.age <= 35 THEN '25-35'  
                                                            WHEN u.age <= 50 THEN '35-50' 
                                                            WHEN u.age > 50 THEN '50+'
                                                        END as age
    FROM df_usert as u
    JOIN df_pint as p
    ON u.ind == p.ind 
    WHERE YEAR(u.date_joined) > 2014 AND YEAR(u.date_joined) < 2021
)
GROUP BY age, year
ORDER BY year DESC
""" 

result = spark.sql(query) 
result.show(result.count())
