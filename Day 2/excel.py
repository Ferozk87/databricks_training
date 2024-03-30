# Databricks notebook source
df=spark.read.format("com.crealytics.spark.excel").load("dbfs:/mnt/cloudthats3/raw/emp.xlsx",header=True)
