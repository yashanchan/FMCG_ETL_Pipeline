# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")



# COMMAND ----------

catalog  = dbutils.widgets.get("catalog")
data_source  = dbutils.widgets.get("data_source")

print(catalog,data_source)

# COMMAND ----------

base_path = f's3://sports-firm/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df = (spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(base_path).withColumn("read_timestamp",F.current_timestamp())
      .select("*","_metadata.file_name","_metadata.file_size"))
display(df.head(10))

# COMMAND ----------

df.describe()
df.printSchema()

# COMMAND ----------

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Processing

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.display(20)

# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().where("count > 1")
df_duplicates.display()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(["customer_id"])

# COMMAND ----------

display(
    df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name")))
)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",F.trim(F.col("customer_name")))

# COMMAND ----------

df_silver.select('city').distinct().display()

# COMMAND ----------

city_mapping = {
    'NewDheli': 'New Delhi',
    'NewDelhee': 'New Delhi',
    'NewDelhi': 'New Delhi',

    'Bengaluruu': 'Bengaluru',
    'Bengalore': 'Bengaluru',

    'Hyderabadd': 'Hyderabad',
    'Hyderbad': 'Hyderabad'
}

allowed = ["New Delhi","Bengaluru","Hyderabad"]

df_silver = (df_silver
             .replace(city_mapping,subset=["city"])
             .withColumn(
                 "city",
                 F.when(F.col("city").isNull(),None)
                  .when(F.col("city").isin(allowed),F.col("city"))
                  .otherwise(None)
             ))

df_silver.select("city").distinct().display()

# COMMAND ----------

df_silver = df_silver.withColumn(
    "customer_name",
    F.when(F.col("customer_name").isNull(),None)
    .otherwise(F.initcap("customer_name"))
)

df_silver.select("customer_name").display()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).display(truncate=True)

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).display(truncate=True)

# COMMAND ----------

customer_city_fix = {
    789403 : "New Delhi",
    789603 : "Hyderabad",
    789420 : "Bengaluru",
    789521 : "Hyderabad"
    }

df_fix = spark.createDataFrame(
    [(a,b) for a,b in customer_city_fix.items()],
    ["customer_id","fixed_city"]
)

df_fix.display()

# COMMAND ----------

df_silver = (
    df_silver.join(df_fix, "customer_id","left").withColumn("city",F.coalesce("city","fixed_city")).drop("fixed_city")
)

# COMMAND ----------

df_silver.display()

# COMMAND ----------

null_customer_names = ['Sprintx Nutrition','Zenathlete Foods','Primefuel Nutrition','Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).display(truncate=True)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id",df_silver["customer_id"].cast("string"))
df_silver.printSchema()

# COMMAND ----------

df_silver = (
    df_silver.withColumn("customer",F.concat_ws(" - ","customer_name",F.coalesce(F.col("city"),F.lit("Unknown"))
))
    .withColumn("market",F.lit("India"))
    .withColumn("platfrom",F.lit("Sports Bar"))
    .withColumn("channel",F.lit("Acquisition"))
)

df_silver.display(10)

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", F.col("customer_id").cast("string"))

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .option("overwriteSchema","true")\
    .option("mergeSchema","true")\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Processing

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

df_gold = df_silver.select("customer_id","customer_name","customer","city","market","platfrom","channel")

# COMMAND ----------

df_gold.display(10)

# COMMAND ----------

df_gold.write\
    .format("delta")\
    .option("deltaenableChangeDataFeed","true")\
    .mode("overwrite")\
    .option("overwriteSchema","true")\
    .option("mergeSchema","true")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

df_gold.printSchema()

# COMMAND ----------

df_child_customers = spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    F.col("customer_name").alias("customer"),
    "market",
    F.col("platfrom").alias("platform"),
    "channel"
)

# COMMAND ----------

delta_table = DeltaTable.forName(spark,"fmcg.gold.dim_customers")

# COMMAND ----------

delta_table.alias("target").merge(
    source = df_child_customers.alias("source"),
    condition = "target.customer_code = source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

