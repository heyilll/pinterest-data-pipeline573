<!-- TODO: this file needs expanding. It acts as the front page of your project and most employers probably wont even move past this page on to your actual code. So it needs to be as good as it possibly can be. -->

# pinterest-data-pipeline573
## Project description 
My task was to produce a data pipeline for a mock Pinterest using AWS Cloud that can provide value to their users. 

Firstly, I configured an EC2 instance to use as a Apache Kafka client machine and connected a MSK cluster to a S3 bucket. Then I built an API to send data to the MSK cluster, which will be stored in an S3 bucket, using the connector we have built. To read this data from the S3 bucket into Databricks, I mounted the S3 bucket to my account. From there, the data was cleaned using Spark. I created an Airflow DAG that triggers a Databricks Notebook to be run on a specific schedule on AWS MWAA. I also learned how to send streaming data to Kinesis, read and perform data cleaning on the data in Databricks and write the streaming data to Delta Tables.

## Diagram
![Copy of CloudPinterestPipeline](https://github.com/heyilll/pinterest-data-pipeline573/assets/117127128/868960ba-1353-480c-a3fc-4d036d2880c0)


<!-- TODO: A section here outlining the steps you used to complete the project. -->
## Steps
1. Create an Apache cluster using AWS MSK
2. Create a client machine for the cluster
   1. Enable client machine to connect to the cluster
   2. Install Kafka on the client machine
   3. Create topics on the Kafka cluster
3. Create an API using API Gateway  
4. Connect the Apache cluster to AWS S3 bucket
5. Batch processing
   1. Mount the S3 bucket
   2. Clean data using Apache Spark 
   3. Query the data using Apache Spark 
   4. Automate workflow of notebook on Databricks
   5. using MWAA
6. Stream processing
   1. Create data streams on Kinesis
   2. Create API proxy for uploading data to streams
   3. Send data to the Kinesis streams
   4. Process/clean the streaming data in Databricks 

## Project dependencies 
In order to run this project, the following modules need to be installed:

python 
sqlalchemy
requests
pymysql
 
## File structure
```
pinterest-data-pipeline573
├── 0e90e0175553_dag.py
├── README.md
├── batch_databricks_sql.ipynb
├── streaming_databricks.ipynb
├── database_utils.py
├── user_posting_emulation_streaming.py
└── user_posting_emulation.py
``` 
<!-- TODO: a section here on your queries from databricks notebooks, showing the sort of questions you can answer with the cleaned data. Some sample screenshots of the outputted tables would also be great. -->
## Queries  
Using the cleaned data from the pipeline, SQL queries can provide useful insights that can be utilised for the data needs of the employer. real-time analysis leveraging a Spark cluster on Databricks. For example, the following are the types of queries that can be answered:

Find the user with the most followers in each country


Based on the above query, find the country with the user with most followers


Find the most popular category for different age groups


## License
N/A
