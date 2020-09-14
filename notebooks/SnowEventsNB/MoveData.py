# Databricks notebook source
snowEventParq="/mnt/SnowEvent/2020/SnowEvent.parquet"
df= sqlContext.sql("Select * from snowEvent")
df.write.option("compression", "snappy").mode("overwrite").parquet(snowEventParq)  


# COMMAND ----------

snowEventParq="/mnt/SnowEvent/2020/SnowEventDetails.parquet"
df= sqlContext.sql("Select * from SnowEventDetails")
df.write.option("compression", "snappy").mode("overwrite").parquet(SnowEventDetailsParq)  


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEvent
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,StartDate TIMESTAMP,EndDate TIMESTAMP,TotalLocationCount int ,LocationCount int ,SubmissionCount int ,CalculatinDate TIMESTAMP,LastSubmissionDate TIMESTAMP,NextEventId int,NextEventDate TIMESTAMP,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING , Action STRING) 
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEventDetails
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,Province STRING ,City STRING ,Address STRING ,Location STRING ,Lat STRING ,Long STRING ,SnowResponsibility STRING ,SubmissionDate TIMESTAMP) 
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into SnowEvent select * from SnowEventDetailsTemp

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into SnowEventDetails select * from SnowEventDetailsTemp