import requests
import json
import os
from google.cloud import pubsub_v1
import logging

# Create and configure logger
logging.basicConfig(filename='filelog.log',
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

TOPIC_PATH = "projects/grounded-region-463501-n1/topics/crypto-data"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_keys.json"



class CryptoData():
    def __init__(self,api_key):
        self.api_key = api_key
        
    def fetch_cryptoData(self):
        url = "https://rest.coincap.io/v3/assets"
        params = {
            "apiKey": self.api_key
        }
        
        response = requests.get(url,params=params)
        logger.info(f"Fetching crypto data from: {response.url}")
        
        if response.status_code == 200:
            return response.json().get("data", [])
        else:
            logger.info(f"Error fetching data from: {response.status_code}|{response.text}")
            return []
        

def publish_cryptoData(data):
    
    publisher = pubsub_v1.PublisherClient()
    message = json.dumps(data).encode("utf-8")
    future = publisher.publish(TOPIC_PATH,data=message)
    logger.info(f"The message is publishing into pubsub")
    future.result()
    logger.info(f"The message is published into pubsub")
        





if __name__ == "__main__":
    with open("crypto_data_key.json") as infile:
        json_obj = json.load(infile)
        
    api_key = json_obj["CRYPTO_API_KEY"]
    crypto_data_call = CryptoData(api_key)
    crypto_data = crypto_data_call.fetch_cryptoData()
    logger.info(f'Total data printed: {len(crypto_data)}')
    
    if crypto_data:
        for data in crypto_data:
            logger.info(f'The crypto_data are: {data}')
            publish_cryptoData(data)
    else:
        logger.info(f"No data is retrieved.")
    
    
    