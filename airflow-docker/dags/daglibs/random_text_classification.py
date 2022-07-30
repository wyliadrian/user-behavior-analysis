import sys
from datetime import datetime

import pyspark
from pyspark.ml.feature import StopWordsRemover, Tokenizer
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, lit

src_path = sys.argv[1]
dst_path = sys.argv[2]

spark = SparkSession.builder.appName("sample classifier").getOrCreate()

# read input
df_raw = spark.read.option("header", True).csv(src_path)

# Tokenize text
tokenizer = Tokenizer(inputCol = "review_str", outputCol = "review_token")
df_tokens = tokenizer.transform(df_raw).select("cid", "review_token")

# Remove stop words
remover = StopWordsRemover(inputCol = "review_token", outputCol = "review_clean")
df_clean = remover.transform(df_tokens).select("cid", "review_clean")

# Check presence of good
df_out = df_clean.select("cid", array_contains(df_clean.review_clean, "good").alias("positive_review"))
df_fin = df_out.withColumn("insert_date", lit(datetime.now()))

# Write the output to a parquet file
df_fin.write.mode("overwrite").parquet(dst_path)