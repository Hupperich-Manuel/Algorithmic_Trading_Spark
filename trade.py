import requests, json
from config import *
import time


BASE_URL = "https://paper-api.alpaca.markets"
ACCOUNT_URL = "{}/v2/account".format(BASE_URL)
ORDERS_URL = "{}/v2/orders".format(BASE_URL)
HEADERS = {"APCA-API-KEY-ID": API_KEY , "APCA-API-SECRET-KEY": SECRET_KEY}


def create_orders(row):
    
    """
    This function gathers the real time data from spark and sends the request of buyning x amount BTC/USD at the market price.
    """
    
    time.sleep(5)
        
    row = list(row)
        
    data = {
        "symbol":row[0],
        "qty":row[1],
        "side":row[2],
        "type":row[3],
        "time_in_force":row[4]
    }

    try:
        r = requests.post(ORDERS_URL, json=data, headers=HEADERS)
        print("Order requested to Alpaca: {} at {}:{} h -> prediction:{}".format(data["side"], int(row[7]), int(row[6]), round(row[5], 3)))
    except IndexError:
        print("Error sending the order")
    
    return json.loads(r.content)

#response = create_orders("AAPL", 100, "buy", "market", "gtc")
#print(response)