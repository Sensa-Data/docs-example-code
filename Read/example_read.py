from datetime import datetime, timedelta
from dotenv import load_dotenv
import httpx
import json
import os

load_dotenv()

DB = {
        "customer_id": os.getenv("ORG"),
        "key": os.getenv("KEY"),
        "bucket": os.getenv("BUCKET")
}

PARAMS = {
        "db": DB,
        "query": {
            "system": "Weather_Station_OSLO",
            "select": ["time", "value", "unit"],
            "time_range": {
                "start": str(datetime.utcnow() - timedelta(days=1)),
                "stop": str(datetime.utcnow())
            },
            "filter_tags": [
                {"section": "BLINDERN"},
                {"equipment": "Weather_Station"},
                {"subunit": "Rain_Gauge"},
                {"id": "SN18700:0"}
            ],
            "limit": 10,
        }
}

def main():
    with httpx.Client() as client:
        url = "https://api.sensadata.io/read/"
        params = json.dumps(PARAMS)
        response = client.post(url, data=params)

        if response.status_code == 200:
            with open("data.json", "w") as f:
                f.write(response.json())
        else:
            print(response)

if __name__ == "__main__":
    main()
