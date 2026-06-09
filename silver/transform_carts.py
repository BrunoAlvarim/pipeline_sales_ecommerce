from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType, TimestampType
from delta.tables import DeltaTable

from func.schema_silver import cart_schema
from func.clear_string import clean_str_lower, clean_str_upper
from func.get_logging import get_logger

logging = get_logger("silver_transform_products")

def read_bronze():
    source_table = "ecommerce.bronze.carts"
    logging.info(f"Lendo tabela {source_table}")
    df_raw = spark.read.table(source_table)
    max_date = df_raw.agg(f.max("ingestion_timestamp")).first()[0]

    if max_date is None:
        raise ValueError(f"Base {source_table} Vazia")

    logging.info(f"Última ingestion_timestamp encontrada:{max_date}")
    df = (
            df_raw
            .filter(f.col("ingestion_timestamp") == max_date)
            .select(
                f.from_json(f.col("raw_json"), cart_schema).alias("data"),
                f.col("ingestion_timestamp"),
                f.col("row_id"),
            )
            .select("data.*", "ingestion_timestamp", "row_id")
        )                                                                                                                                                                                    
    return df

def transform_to_silver(df):
    logging.info("Aplicando transformações silver...")
    df = (
        df
        .withColumn(
            "product",
            f.explode("products")
        )
        .select(
            f.col("id").alias("cart_id"),
            f.sha2(
                f.concat_ws(
                    "-", f.col("id"), f.col("product.id")
                ),
                256
            ).alias("hash_id"),
            f.col("userId").alias("client_id"),

            f.col("product.id").alias("product_id"),
            f.col("product.quantity").alias("quantity"),
            f.col("product.price").alias("unit_price"),

            f.col("product.total").alias("gross_amount"),
            f.col("product.discountedTotal").alias("net_amount"),

            f.col("total").alias("cart_total"),
            f.col("discountedTotal").alias("cart_discounted_total"),

            f.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp"),
            f.col("row_id")
        )
    )
    return df

def dedup(df):
    logging.info("iniciando dedup")

    partition_by = Window.partitionBy("hash_id").orderBy(
        f.col("ingestion_timestamp").desc()
    )
    df = (
        df.withColumn("dp",f.row_number().over(partition_by))
        .filter(f.col("dp") == 1)
        .drop("dp")
    )
    return df

def merge(df):
    logging.info("iniciando merge")

    df_dim_products = spark.read.table("ecommerce.silver.dim_product")
    df_dim_client = spark.read.table("ecommerce.silver.dim_client")

    df = (
        df.alias("df")
        .join(
            df_dim_products.select("product_id", "sk_product").alias("product"),
            f.col("product.product_id") == f.col("df.product_id"),
            how = "inner"
        )
        .join(
            df_dim_client.select("client_id","sk_client").alias("client"),
            f.col("client.client_id") == f.col("df.client_id"),
            how = "inner"
        ).select(
            "df.*",
            "product.sk_product",
            "client.sk_client"
        )
    )

    EXCLUDE_COLUMNS = [
        "product_id",
        "client_id",
        "sk_product"
        "cart_id",
        "hash_id",
        "ingestion_timestamp",
        "updated_timestamp"
    ]

    cols = df.columns

    merge_cols = [col for col in cols if col not in EXCLUDE_COLUMNS]

    update_cols = {col: f"tmp.{col}"for col in merge_cols}
    insert_cols = {col: f"tmp.{col}"for col in merge_cols}


    update_cols["updated_timestamp"] = "current_timestamp()"

    insert_cols["sk_product"] = "tmp.sk_product"
    insert_cols["cart_id"] = "tmp.cart_id"
    insert_cols["hash_id"] = "tmp.hash_id"
    
    insert_cols["ingestion_timestamp"] = "tmp.ingestion_timestamp"

    dim = DeltaTable.forName(spark, "ecommerce.silver.ft_sale")

    (
        dim.alias("ft")
        .merge(
            df.alias("tmp"),
            condition = "ft.hash_id = tmp.hash_id"
        )
        .whenMatchedUpdate(set = update_cols)
        .whenNotMatchedInsert(values = insert_cols)
        .execute()
    )
    metrics = (
        spark.sql(f"DESCRIBE HISTORY ecommerce.silver.ft_sale LIMIT 1")
             .select("operationMetrics")
             .first()[0]
    )
    row_inserted = metrics.get("numTargetRowsInserted","n/a")
    row_updated = metrics.get("numTargetRowsUpdated","n/a")
    source_rows = metrics.get("numSourceRows","n/a")

    logging.info(
        f"Merge concluído | row_inserted:{row_inserted} | row_updated: {row_updated} | source_rows: {source_rows}"
    )    

    return None

def run():

    logging.info("iniciando processamento silver de products")

    try:
        df_bronze = read_bronze()
        
    except Exception as e:
        logging.error("FALHA na leitura da bronze: %s", e)
        raise RuntimeError("Etapa read_bronze falhou") from e
    try:
        df_silver = transform_to_silver(df = df_bronze)   
    except Exception as e:
        logging.error("FALHA na leitura da bronze: %s", e)
        raise RuntimeError("Etapa transform_to_silver falhou") from e

    try:
        df_silver = dedup(df = df_silver)
    except Exception as e:
        logging.error("FALHA na leitura da bronze: %s", e)
        raise RuntimeError("Etapa dedup falhou") from e
    try:
        df_silver = merge(df = df_silver)
    except Exception as e:
        logging.error("FALHA na leitura da bronze: %s", e)
        raise RuntimeError("Etapa merge falhou") from e    
run()