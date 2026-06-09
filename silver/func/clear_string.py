from pyspark.sql.functions import trim,upper,lower,col
from pyspark.sql.types import StringType

def clean_str_upper(col_name):
    return trim(upper(col(col_name))).cast(StringType())

def clean_str_lower(col_name):
    return trim(upper(col(col_name))).cast(StringType())