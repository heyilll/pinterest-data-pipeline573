from time import sleep
import random  
from database_utils import * 
random.seed(100) 
invoke_url = "https://skljjq7iub.execute-api.us-east-1.amazonaws.com/test/streams/"
 
new_connector = AWSDBConnector()
 
def run_infinite_post_data_loop():
    '''
    Retrieves data from an engine, one entry at a time, and sends it to a URL using the send() method. This is done three times each loop to retrieve and send for each type of data, 
    from pinterest data to geographic and user data. It then repeats this in a loop until the user stops it manually. 

            Parameters:
                    N/A

            Returns:
                    N/A
    ''' 
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            pin_result = new_connector.retrieve(connection, 'pinterest_data', random_row)    
            geo_result = new_connector.retrieve(connection, 'geolocation_data', random_row)
            user_result = new_connector.retrieve(connection, 'user_data', random_row)
                
            new_connector.sendtostream(pin_result, 'streaming-0e90e0175553-pin/record', 'streaming-0e90e0175553-pin')
            new_connector.sendtostream(geo_result, 'streaming-0e90e0175553-geo/record', 'streaming-0e90e0175553-geo')
            new_connector.sendtostream(user_result, 'streaming-0e90e0175553-user/record', 'streaming-0e90e0175553-user')
  
if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')  
    