import requests 
import random 
import json
import sqlalchemy
from sqlalchemy import text
import yaml

random.seed(100)
invoke_url = "https://skljjq7iub.execute-api.us-east-1.amazonaws.com/test/topics/"

class AWSDBConnector:

    def __init__(self): 
        with open('db_creds.yaml', 'r') as f:
            creds = yaml.safe_load(f)
            
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT'] 
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine 
                    
    def retrieve(self, connection, table, random_row):
        '''
        Sends the payload to the corresponding address that is correct for the specific type 
        of payload. For example, if the payload contains the geographic information of pins, then the 
        correct address would end in .geo.

                Parameters:
                        payload (str): table data
                        add (str): end part of address

                Returns:
                        result (dict): Entry retrieved from connection
        '''         
        user_string = text(f"SELECT * FROM {table} LIMIT {random_row}, 1")
        selected_row = connection.execute(user_string)
        
        for row in selected_row:
            result = dict(row._mapping)       
        return result          
    
    def send(self, payload, add): 
        '''
        Sends the payload to the corresponding address that is correct for the specific type 
        of payload. For example, if the payload contains the geographic information of pins, then the 
        correct address would end in .geo.

                Parameters:
                        payload (str): table data
                        add (str): end part of address

                Returns:
                        N/A
        ''' 
        iu = invoke_url + add
        headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
        jsonpayload = json.dumps({
            "records": [
                {   
                    "value": payload
                }
            ]
        }, default=str) 
        response = requests.request("POST", iu, headers=headers, data=jsonpayload) 
        print(response.status_code)
        
    def sendtostream(self, payload, add, stream):
        '''
        Sends the payload to the corresponding address and stream that is correct for the specific type 
        of payload. For example, if the payload contains the geographic information of pins, then the 
        correct address and stream would end in .geo.

                Parameters:
                        payload (str): table data
                        add (str): end part of address
                        stream (str): name of Kinesis stream
                Returns:
                        N/A
        ''' 
        iu = invoke_url + add
        headers = {'Content-Type': 'application/json'}
        
        payl = json.dumps({
            "StreamName": stream,
            "Data": payload,
            "PartitionKey": "test"
        }, default=str) 
        
        response = requests.request("PUT", iu, headers=headers, data=payl) 
        print(response.status_code)      