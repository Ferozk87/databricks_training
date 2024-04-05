# Databricks notebook source
df=spark.read.option("multiLine",True).json("path")

# COMMAND ----------

df=spark.read.json("path",multiLine=True)
