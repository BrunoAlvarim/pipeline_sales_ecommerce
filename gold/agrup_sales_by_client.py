from pyspark.sql import functions as f
from pyspark.sql.types import DoubleType
from delta.tables import DeltaTable

from func.get_logging import get_logger

logging = get_logger("gold_sales_by_client")


def read_silver():
    ft_sale_table    = "ecommerce.silver.ft_sale"
    dim_client_table = "ecommerce.silver.dim_client"

    logging.info(f"Lendo tabela {ft_sale_table}")
    df_ft_sale = spark.read.table(ft_sale_table)

    logging.info(f"Lendo tabela {dim_client_table}")
    df_dim_client = spark.read.table(dim_client_table)

    return df_ft_sale, df_dim_client


def transform_to_gold(df_ft_sale, df_dim_client):

    logging.info("Aplicando transformações gold...")

    df = (
        df_ft_sale
        .join(df_dim_client, df_ft_sale.sk_client == df_dim_client.sk_client, "inner")
        .groupBy(
            df_dim_client.client_id,
            df_dim_client.client_first_name,
            df_dim_client.client_last_name,
        )
        .agg(
            f.sum("unit_price").cast(DoubleType()).alias("total_carrinho")
        )
    )

    return df


def data_quality(df):

    total = df.count()
    logging.info("Total de registros recebidos: %d", total)

    if total == 0:
        raise ValueError("DataFrame vazio.")

    nulls_id = df.filter(f.col("client_id").isNull()).count()

    if nulls_id > 0:
        pct_nulls = nulls_id / total
        logging.warning(
            "Removendo %d registros com client_id nulo (%.2f%%)",
            nulls_id,
            pct_nulls * 100,
        )

    df_clean = df.filter(f.col("client_id").isNotNull())

    logging.info("Registros válidos: %d", df_clean.count())
    return df_clean


def merge(df):

    target_table = "ecommerce.gold.sales_by_client"

    logging.info(f"Iniciando merge em: {target_table}")

    delta_table = DeltaTable.forName(spark, target_table)

    EXCLUDE_COLS = {"client_id"}

    cols = df.columns
    merge_cols = [col for col in cols if col not in EXCLUDE_COLS]

    update_set  = {col: f"tmp.{col}" for col in merge_cols}
    insert_vals = {col: f"tmp.{col}" for col in merge_cols}

    insert_vals["client_id"] = "tmp.client_id"

    (
        delta_table.alias("gold")
        .merge(
            source    = df.alias("tmp"),
            condition = "gold.client_id = tmp.client_id",
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
    row_inserted = metrics.get("numTargetRowsInserted", "n/a")
    row_updated  = metrics.get("numTargetRowsUpdated",  "n/a")
    source_rows  = metrics.get("numSourceRows",         "n/a")

    logging.info(
        f"Merge concluído | row_inserted:{row_inserted} | row_updated:{row_updated} | source_rows:{source_rows}"
    )


def run():

    logging.info("Iniciando processamento gold de sales_by_client")

    try:
        df_ft_sale, df_dim_client = read_silver()
    except Exception as e:
        logging.error("FALHA na leitura da silver: %s", e)
        raise RuntimeError("Etapa read_silver falhou") from e

    try:
        df_gold = transform_to_gold(df_ft_sale, df_dim_client)
    except Exception as e:
        logging.error("FALHA na transformação: %s", e)
        raise RuntimeError("Etapa TRANSFORMAÇÃO falhou") from e

    try:
        df_gold = data_quality(df=df_gold)
    except Exception as e:
        logging.error("FALHA na validação de qualidade: %s", e)
        raise RuntimeError("Etapa QUALIDADE falhou") from e

    try:
        merge(df=df_gold)
    except Exception as e:
        logging.error("FALHA no merge para gold: %s", e)
        raise RuntimeError("Etapa MERGE falhou") from e

    logging.info("Processamento concluído com sucesso")


run()