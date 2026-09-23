import requests
import time
import json

URL = "https://api.exchange.coinbase.com/products/BTC-USD/ticker"

while True:
    try:
        response = requests.get(URL, timeout=10)

        if response.status_code == 200:
            data = response.json()

            # Print one JSON record per line
            print(json.dumps(data), flush=True)

        else:
            print(json.dumps({
                "error": f"HTTP {response.status_code}"
            }), flush=True)

    except Exception as e:
        print(json.dumps({
            "error": str(e)
        }), flush=True)

    time.sleep(3)
