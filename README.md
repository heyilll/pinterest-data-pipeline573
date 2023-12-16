# pinterest-data-pipeline573
## Project description 
My task was to produce a data pipeline for a mock Pinterest using the AWS Cloud that can provide value to their users. Firstly, I configured an EC2 instance to use as a Apache Kafka client machine and connected a MSK cluster to a S3 bucket. Then I built an API to send data to the MSK cluster, which will be stored in an S3 bucket, using the connector we have built. To read this data from the S3 bucket into Databricks, I mounted the S3 bucket to my account. From there, the data was cleaned using Spark. I created an Airflow DAG that triggers a Databricks Notebook to be run on a specific schedule on AWS MWAA. I also learned how to send streaming data to Kinesis, read and perform data cleaning on the data in Databricks and write the streaming data to Delta Tables.

## Diagram


## Installation
N/A - This repo contains the code used in Databricks notebooks.

## Usage 
Download repo and extract zip to preferred location. 

## File structure
```
pinterest-data-pipeline573
├── 0e90e0175553_dag.py
├── README.md
├── databricks.py
└── databricks_sql.py
``` 

## License
N/A