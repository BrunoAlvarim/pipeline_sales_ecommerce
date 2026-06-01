import httpx
import json
import pandas as pd
from datetime import datetime
from uuid import uuid4

from bronze.func.get_logging import get_logger

logging = get_logger("bronze_extract_products")

try:
    logging.info("iniciando extração de produtos")
    with httpx.Client(timeout=30) as client:
        url = "https://dummyjson.com/products"
        response = client.get(url)
        products = response.json()["products"]

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    pdf = pd.DataFrame({
        "raw_json": [json.dumps(u) for u in products],
        "ingestion_timestamp": timestamp,
        "row_id" : [str(uuid4()) for _ in range(len(products))],
        "source": url
    })

    df = spark.createDataFrame(pdf)
    
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable("ecommerce.bronze.products")
    )
    logging.info(f"{df.count()} linhas gravadas na base bronze.products")
except Exception as e:
    logging.error(e)