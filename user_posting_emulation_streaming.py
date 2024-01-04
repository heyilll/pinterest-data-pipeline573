import requests
import json
from time import sleep
import random
import sqlalchemy
from sqlalchemy import text

random.seed(100)
# invoke url for one record, if you want to put more records replace record with records
invoke_url = "https://skljjq7iub.execute-api.us-east-1.amazonaws.com/test/streams/"

class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping) 
                
            send(pin_result, 'streaming-0e90e0175553-pin/record', 'streaming-0e90e0175553-pin')
            send(geo_result, 'streaming-0e90e0175553-geo/record', 'streaming-0e90e0175553-geo')
            send(user_result, 'streaming-0e90e0175553-user/record', 'streaming-0e90e0175553-user')

def send(payload, add, stream):
    iu = invoke_url + add
    headers = {'Content-Type': 'application/json'}
    
    payl = json.dumps({
        "StreamName": stream,
        "Data": payload,
        "PartitionKey": "test"
    }, default=str) 
    
    response = requests.request("PUT", iu, headers=headers, data=payl)
    print(payl)   
    print(response.status_code)  
    
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')  
    