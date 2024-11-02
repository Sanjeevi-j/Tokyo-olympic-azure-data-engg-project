# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "9e177baa-b872-4e81-bfb3-352c3b67a2b2",
"fs.azure.account.oauth2.client.secret": 'Y6b8Q~TTt3Zyumb5asfUWJCV5ffmwLOInDvTxaTe',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/607c6aae-5d9d-4d89-af3c-b83c19814d74/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolymbicdata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

# to dispplay whether the file is mounted in the correct path or not
display(dbutils.fs.ls("/mnt/tokyoolymic"))

# COMMAND ----------

spark

# COMMAND ----------

athletes = spark.read.format("csv").option("header","true").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/Athletes.csv")
Coaches = spark.read.format("csv").option("header","true").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/Coaches.csv")
EntriesGender = spark.read.format("csv").option("header","true").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/EntriesGender.csv")
Medals = spark.read.format("csv").option("header","true").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/Medals.csv")
Teams = spark.read.format("csv").option("header","true").option("inferSchema","True").load("/mnt/tokyoolymic/raw-data/Teams.csv")

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

Coaches.show()

# COMMAND ----------

Coaches.printSchema()

# COMMAND ----------

EntriesGender.show()

# COMMAND ----------

EntriesGender.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

EntriesGender = EntriesGender.withColumn("Female", col("Female").cast(IntegerType())) \
    .withColumn("Male", col("Male").cast(IntegerType())) \
    .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

Medals.show()

# COMMAND ----------

Medals.printSchema()

# COMMAND ----------

Teams.show()

# COMMAND ----------

Teams.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
top_gold_medal_countries = Medals.orderBy("Gold", ascending=False).select("TeamCountry","Gold")
display(top_gold_medal_countries)

# COMMAND ----------

# Calculate the average number of entries by gender for each employee
average_entries_by_gender = (
    EntriesGender
    .withColumn('Average_Female', EntriesGender['Female'] / EntriesGender['Total'])
    .withColumn('Avg_Male', EntriesGender['Male'] / EntriesGender['Total'])
)
display(average_entries_by_gender)

# COMMAND ----------

athletes.write.mode("overwrite").option("header",'True').csv("/mnt/tokyoolymic/transformed-data/athletes")

# COMMAND ----------

Coaches.write.mode("overwrite").option("header",'True').csv("/mnt/tokyoolymic/transformed-data/Coaches")
EntriesGender.write.mode("overwrite").option("header",'True').csv("/mnt/tokyoolymic/transformed-data/EntriesGender")
Medals.write.mode("overwrite").option("header",'True').csv("/mnt/tokyoolymic/transformed-data/Medals")
Teams.write.mode("overwrite").option("header",'True').csv("/mnt/tokyoolymic/transformed-data/Teams")

# COMMAND ----------

