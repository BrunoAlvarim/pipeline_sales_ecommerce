from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DateType, DoubleType, TimestampType
from delta.tables import DeltaTable

from func.schema_silver import user_schema
from func.clear_string import clean_str_lower, clean_str_upper
from func.get_logging import get_logger

logging = get_logger("silver_transform_users")

def read_bronze():
    source_table = "ecommerce.bronze.users"
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
                f.from_json(f.col("raw_json"), user_schema).alias("data"),
                f.col("ingestion_timestamp"),
                f.col("row_id"),
            )
            .select("data.*", "ingestion_timestamp", "row_id")
        )
    return df

def transform_to_silver(df):

    logging.info("Aplicando transformações silver...")
    df = df.select(
        f.col("id").cast(IntegerType()).alias("client_id"),
        clean_str_upper("firstName").alias("client_first_name"),
        clean_str_upper("lastName").alias("client_last_name"),
        clean_str_upper("maidenName").alias("client_maiden_name"),
        f.col("age").cast(IntegerType()).alias("client_age"),
        f.when(f.col("age").isNull(), None)
         .when(f.col("age").cast(IntegerType()) >= 18, True)
         .otherwise(False)
         .alias("is_adult"),

        f.when(clean_str_upper("gender") == "MALE",   "M")
         .when(clean_str_upper("gender") == "FEMALE", "F")
         .when(clean_str_upper("gender") == "OTHER",  "O")
         .otherwise(None)
         .alias("client_gender"),
        f.col("birthDate").cast(DateType()).alias("client_birthdate"),
        f.trim(f.lower(f.col("email"))).alias("client_email"),
        f.trim(f.col("username")).alias("client_username"),
        f.trim(f.col("phone")).alias("client_phone"),
        f.when(
            f.col("phone").startswith("+"),
            f.concat(
                f.lit("+"),
                f.regexp_replace(f.substring(f.col("phone"), 2, 100), r"[^0-9]", ""),
            ),
        ).otherwise(
            f.regexp_replace(f.col("phone"), r"[^0-9]", "")
        ).alias("client_phone_clean"),
        clean_str_upper("address.address").alias("client_address"),
        clean_str_upper("address.city").alias("client_city"),
        clean_str_upper("address.state").alias("client_state"),
        clean_str_upper("address.country").alias("client_country"),
        f.trim(f.col("address.stateCode")).alias("client_state_code"),
        f.trim(f.col("address.postalCode")).alias("client_postal_code"),
        f.col("address.coordinates.lat").cast(DoubleType()).alias("client_latitude"),
        f.col("address.coordinates.lng").cast(DoubleType()).alias("client_longitude"),
        f.trim(f.upper(f.col("university"))).alias("university"),
        clean_str_upper("company.department").alias("company_department"),
        clean_str_upper("company.name").alias("company_name"),
        clean_str_upper("company.title").alias("company_title"),
        clean_str_upper("company.address.address").alias("company_address"),
        clean_str_upper("company.address.city").alias("company_city"),
        clean_str_upper("company.address.state").alias("company_state"),
        f.trim(f.col("company.address.stateCode")).alias("company_state_code"),
        f.trim(f.col("company.address.postalCode")).alias("company_postal_code"),
        clean_str_upper("company.address.country").alias("company_country"),
        f.col("company.address.coordinates.lat").cast(DoubleType()).alias("company_latitude"),
        f.col("company.address.coordinates.lng").cast(DoubleType()).alias("company_longitude"),
        f.trim(f.lower(f.col("role"))).alias("role"),
        f.col("ingestion_timestamp").cast(TimestampType()),
        f.col("row_id"),
    )
    return df

def dedup(df):

    logging.info(f"Deduplicando por client_id")

    window_spec = Window.partitionBy("client_id").orderBy(
        f.col("ingestion_timestamp").desc()
    )

    df = (
            df.withColumn("dp", f.row_number().over(window_spec))
            .filter(f.col("dp") == 1)
            .drop("dp")
        )
    return df

def data_quality(df):

    total = df.count()
    logging.info("Total de registros recebidos: %d", total)

    if total == 0:
        raise ValueError(f"DataFrame vazio.")

    nulls_id = df.filter(f.col("client_id").isNull()).count()
    pct_nulls = nulls_id / total

    if nulls_id > 0:
        logging.warning("Removendo %d registros com client_id nulo (%.2f%%)", nulls_id, pct_nulls * 100)

    df_clean = df.filter(f.col("client_id").isNotNull())

    invalid_emails = df_clean.filter(
        f.col("client_email").isNotNull()
        & ~f.col("client_email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    ).count()

    if invalid_emails > 0:
        logging.warning(f"Encontrados {invalid_emails} e-mails com formato inválido (mantidos, apenas log)")

    logging.info(f"registros validos {df_clean.count()}")
    return df_clean

def merge(df):

    target_table = "ecommerce.silver.dim_client"
    delta_table = DeltaTable.forName(spark, target_table)

    logging.info(f"Iniciando merge em: {target_table}")

    EXCLUDE_COLS = {
        "client_id",
        "ingestion_timestamp",
        "updated_timestamp",
    }

    cols = df.columns
    merge_cols = [col for col in cols if col not in EXCLUDE_COLS]

    update_set  = {col: f"tmp.{col}" for col in merge_cols}
    insert_vals = {col: f"tmp.{col}" for col in merge_cols}

    update_set["updated_timestamp"]    = "current_timestamp()"


    insert_vals["client_id"]             = f"tmp.client_id"
    insert_vals["ingestion_timestamp"] = "tmp.ingestion_timestamp"


    (
        delta_table.alias("dim")
        .merge(
            source = df.alias("tmp"),
            condition = f"dim.client_id = tmp.client_id",
        )
        .whenMatchedUpdate(set=update_set)
        .whenNotMatchedInsert(values=insert_vals)
        .execute()
    )

    metrics = (
        spark.sql(f"DESCRIBE HISTORY {target_table} LIMIT 1")
             .select("operationMetrics")
             .first()[0]
    )
    row_inserted = metrics.get("numTargetRowsInserted","n/a")
    row_updated = metrics.get("numTargetRowsUpdated","n/a")
    source_rows = metrics.get("numSourceRows","n/a")

    logging.info(
        f"Merge concluído | row_inserted:{row_inserted} | row_updated: {row_updated} | source_rows: {source_rows}"
    )

def run():

    logging.info("iniciando processamento silver de client")

    try:
        df_bronze = read_bronze()
    except Exception as e:
        logging.error("FALHA na leitura da bronze: %s", e)
        raise RuntimeError("Etapa read_bronze falhou") from e

    try:
        df_silver = transform_to_silver(df_bronze)
    except Exception as e:
        logging.error("FALHA na transformação: %s", e)
        raise RuntimeError("Etapa TRANSFORMAÇÃO falhou") from e

    try:
        df_silver = data_quality(df = df_silver)
    except Exception as e:
        logging.error("FALHA na validação de qualidade: %s", e)
        raise RuntimeError("Etapa QUALIDADE falhou") from e

    try:
        df_silver = dedup(df = df_silver)
    except Exception as e:
        logging.error("FALHA na deduplicação: %s", e)
        raise RuntimeError("Etapa DEDUPLICAÇÃO falhou") from e

    try:
        merge(df = df_silver)
    except Exception as e:
        logging.error("FALHA no merge para silver: %s", e)
        raise RuntimeError("Etapa MERGE falhou") from e

    logging.info("processamento concluida com sucesso")

run()