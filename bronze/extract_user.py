import httpx
import json
import pandas as pd
from datetime import datetime
from uuid import uuid4

from func.get_logging import get_logger

logging = get_logger("bronze_extract_users")

try:
    logging.info("iniciando extração de cliente")
    url = "https://dummyjson.com/users"
    limit = 100
    skip = 0
    total = 1
    batch = []
    with httpx.Client(timeout=30) as client:
        while skip < total:
            params = {
                "skip": skip,
                "limit": limit
            }
            response = client.get(url=url, params=params)            
            response.raise_for_status()
            data = response.json()
            if "users" not in data:
                logging.warning(f"Resposta inesperada: {data}")
                break

            total = data["total"]
            batch.extend(data["users"])
            skip += limit
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    pdf = pd.DataFrame({
        "raw_json": [json.dumps(u) for u in batch],
        "ingestion_timestamp": timestamp,
        "row_id" : [str(uuid4()) for _ in range(len(batch))],
        "source": f"{url}{params}" 
    })

    df = spark.createDataFrame(pdf)
    
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable("ecommerce.bronze.users")
    )
    logging.info(f"{df.count()} linhas gravadas na base bronze.users")
except Exception as e:
    logging.error(e)