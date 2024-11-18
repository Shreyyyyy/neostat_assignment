# Databricks notebook source
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "a74351f9-74e2-45ab-b90f-8b98ef3aff31",
    "fs.azure.account.oauth2.client.secret": "TSS8Q~ShUNGMZ~usD_KPoqgM-Xy3UEHZ6EHEObZ9",
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/14441ea5-8ae2-4368-af25-82dad83b700d/oauth2/token"
}

dbutils.fs.mount(
    source="abfss://sales-files@storagesalesdatashrey.dfs.core.windows.net",
    mount_point="/mnt/salestest",
    extra_configs=configs
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/salestest"

# COMMAND ----------

spark

# COMMAND ----------

sales.printSchema()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/salestest"

# COMMAND ----------

file_path = "/mnt/salestest/raw-data/sales.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(5)


# COMMAND ----------

from pyspark.sql.functions import col, lit, concat_ws, to_date, year, month

# COMMAND ----------

refined_df = (
    df.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))  # Convert to datetime
    .withColumn("TotalPrice", col("Quantity") * col("Price"))  # Add TotalPrice
    .withColumn("Year", year(col("Date")))  # Extract year
    .withColumn("Month", month(col("Date")))  # Extract month
    .withColumn("MaskedPhone", concat_ws(" ", lit("+1"), lit("***-***"), col("PhoneNumber").substr(-4, 4)))  # Mask Phone
    .withColumn("MaskedCard", concat_ws("", lit("XXXX-XXXX-XXXX-"), col("CreditCardNumber").substr(-4, 4)))  # Mask Card
    .filter(col("Quantity") > 0)  # Remove invalid rows with non-positive Quantity
)

# Step 3: Drop sensitive columns (if masking is enough)
refined_df = refined_df.drop("PhoneNumber", "CreditCardNumber")

# Step 4: Save for Analysis
output_path_csv = "/mnt/salestest/transform-data/"

refined_df.write.mode("overwrite").option("header", "true").csv(output_path_csv)

print(f"Refined CSV data saved to {output_path_csv}")


# COMMAND ----------

refined_df.show(10)

