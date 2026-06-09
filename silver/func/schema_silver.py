from pyspark.sql.types import *


user_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("firstName", StringType(), False),
    StructField("lastName", StringType(), True),
    StructField("maidenName", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), True),
    StructField("username", StringType(), False),
    StructField("password", StringType(), True),
    StructField("birthDate", StringType(), True),
    StructField("image", StringType(), True),
    StructField("bloodGroup", StringType(), True),
    StructField("height", DoubleType(), True),
    StructField("weight", DoubleType(), True),
    StructField("eyeColor", StringType(), True),
    StructField("hair", StructType([
        StructField("color", StringType(), True),
        StructField("type", StringType(), True)
    ]), True),
    StructField("ip", StringType(), True),
    StructField("address", StructType([
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("stateCode", StringType(), True),
        StructField("postalCode", StringType(), True),
        StructField("coordinates", StructType([
            StructField("lat", DoubleType(), True),
            StructField("lng", DoubleType(), True)
        ]), True),
        StructField("country", StringType(), True)
    ]), True),
    StructField("macAddress", StringType(), True),
    StructField("university", StringType(), True),
    StructField("bank", StructType([
        StructField("cardExpire", StringType(), True),
        StructField("cardNumber", StringType(), True),
        StructField("cardType", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("iban", StringType(), True)
    ]), True),
    StructField("company", StructType([
        StructField("department", StringType(), True),
        StructField("name", StringType(), True),
        StructField("title", StringType(), True),
        StructField("address", StructType([
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("stateCode", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("coordinates", StructType([
                StructField("lat", DoubleType(), True),
                StructField("lng", DoubleType(), True)
            ]), True),
            StructField("country", StringType(), True)
        ]), True)
    ]), True),
    StructField("ein", StringType(), True),
    StructField("ssn", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("crypto", StructType([
        StructField("coin", StringType(), True),
        StructField("wallet", StringType(), True),
        StructField("network", StringType(), True)
    ]), True),
    StructField("role", StringType(), True)
])

product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("discountPercentage", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("stock", IntegerType(), True),
    StructField("tags", ArrayType(StringType()), True),
    StructField("brand", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("weight", IntegerType(), True),
    StructField("dimensions", StructType([
        StructField("width", DoubleType(), True),
        StructField("height", DoubleType(), True),
        StructField("depth", DoubleType(), True),
    ]), True),
    StructField("warrantyInformation", StringType(), True),
    StructField("shippingInformation", StringType(), True),
    StructField("availabilityStatus", StringType(), True),
    StructField("reviews", ArrayType(StructType([
        StructField("rating", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("date", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("reviewerEmail", StringType(), True),
    ])), True),
    StructField("returnPolicy", StringType(), True),
    StructField("minimumOrderQuantity", IntegerType(), True),
    StructField("meta", StructType([
        StructField("createdAt", StringType(), True),
        StructField("updatedAt", StringType(), True),
        StructField("barcode", StringType(), True),
        StructField("qrCode", StringType(), True),
    ]), True),
    StructField("images", ArrayType(StringType()), True),
    StructField("thumbnail", StringType(), True),
])

cart_schema = StructType([
    StructField("id", IntegerType()),
    StructField("userId", IntegerType()),
    StructField("total", DoubleType()),
    StructField("discountedTotal", DoubleType()),
    StructField("totalProducts", IntegerType()),
    StructField("totalQuantity", IntegerType()),
    StructField(
        "products",
        ArrayType(
            StructType([
                StructField("id", IntegerType()),
                StructField("title", StringType()),
                StructField("price", DoubleType()),
                StructField("quantity", IntegerType()),
                StructField("total", DoubleType()),
                StructField("discountPercentage", DoubleType()),
                StructField("discountedTotal", DoubleType()),
                StructField("thumbnail", StringType())
            ])
        )
    )
])