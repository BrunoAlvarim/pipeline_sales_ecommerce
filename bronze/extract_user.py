import httpx
import json
import pandas as pd
from datetime import datetime
from uuid import uuid4

from bronze.func.get_logging import get_logger

logging = get_logger("bronze_extract_users")

try:
    logging.info("iniciando extração de cliente")
    with httpx.Client(timeout=30) as client:
        url = "https://dummyjson.com/users"
        response = client.get(url)
        users = response.json()["users"]

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    pdf = pd.DataFrame({
        "raw_json": [json.dumps(u) for u in users],
        "ingestion_timestamp": timestamp,
        "row_id" : [str(uuid4()) for _ in range(len(users))],
        "source": url
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