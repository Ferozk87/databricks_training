# Databricks notebook source
dbutils.fs.mount(
  source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",
  mount_point = "/mnt/<storageaccount>/<containername>",
  extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.unmount("/mnt/sanly/raw")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@sanly.blob.core.windows.net",
  mount_point = "/mnt/sanly/raw",
  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"Key"})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/jsoninputfiles/

# COMMAND ----------

import urllib
access_key = ""
secret_key = ""
encoded_secret_key = urllib.parse.quote(secret_key,"")
aws_bucket_name = ""
mount_name = ""

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

import urllib
access_key = "ACCES_KEY"
secret_key = "SEC_KEy"
encoded_secret_key = urllib.parse.quote(secret_key,"")
aws_bucket_name = "cloudthats3"
mount_name = "cloudthats3"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------

df=spark.read.csv("dbfs:/mnt/sanly/raw/jsoninputfiles/races.csv",header=True,inferSchema=True)

# COMMAND ----------

df.write.parquet("dbfs:/mnt/sanly/raw/proceeddata/naval/races")

# COMMAND ----------


