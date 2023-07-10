import os
import csv
import json
import httpx
import asyncio
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MAX_RECORDS_PER_REQUEST = 1000

def read_data_from_file(path: str):
    db = {
            "customer_id": os.getenv("ORG"),
            "key": os.getenv("KEY"),
            "bucket": os.getenv("BUCKET")
    }

    with open(path, "r") as file:
        records = []
        reader = csv.DictReader(file)
        for idx, row in enumerate(reader):

            records.append({
                "system": "Weather_Station_OSLO",
                "tags": {
                    "section": "BLINDERN",
                    "equipment": "Weather_Station",
                    "subunit": "Rain_Gauge",
                    "id": row["sourceId"]
                },
                "fields": [
                    {
                        "value": row["value"],
                        "name": "Precipitation_Sum",
                        "unit": row["unit"],
                        "error": False
                    }
                    ],
                "time": str(datetime.utcnow())
            })

            if (idx+1) % MAX_RECORDS_PER_REQUEST == 0:
                yield json.dumps({"db": db,"data": records})
                records = []

    if (idx+1) % MAX_RECORDS_PER_REQUEST != 0 and len(records) > 0:
        yield json.dumps({"db": db,"data": records})

async def write_data(client: httpx.AsyncClient, data) -> httpx.Response:
    url = "https://api.sensadata.io/ingest/ingest_many"
    response = await client.post(url, data=data) 
    return response

async def ingest():
    limits = httpx.Limits(max_connections=50, max_keepalive_connections=50)
    async with httpx.AsyncClient(limits=limits) as client:
        requests = []
        for data in read_data_from_file("dataset/HRLY_PRECIP_SUM_OSLO_2013_2023_small.csv"):
            requests.append(asyncio.ensure_future(write_data(client, data)))
        await asyncio.gather(*requests)

if __name__ == "__main__":
    asyncio.run(ingest())
