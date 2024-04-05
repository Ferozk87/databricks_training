# Databricks notebook source
# MAGIC %fs ls 

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://<containername>@<storageaccount>.blob.core.windows.net",
  mount_point = "/mnt/<storageaccount>/<containername>",
  extra_configs = {"fs.azure.account.key.<storageaccount>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://raw@sanly.blob.core.windows.net",
  mount_point = "/mnt/sanly/raw",
  extra_configs = {"fs.azure.account.key.sanly.blob.core.windows.net":"KEY"})

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/sanly/raw/marsh/

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/sanly/raw/marsh/customers.csv`

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
access_key = "ACCESS_KEY"
secret_key = "KEY"
encoded_secret_key = urllib.parse.quote(secret_key,"")
aws_bucket_name = "cloudthats3"
mount_name = "cloudthats3"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/cloudthats3/formula1_raw/

# COMMAND ----------


