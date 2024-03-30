# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

location= "dbfs:/mnt/cloudthats3/formula1_raw"
output_path="dbfs:/mnt/cloudthats3/output_formula1/"
