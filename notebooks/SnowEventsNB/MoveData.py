# Databricks notebook source
applicationID = "360de2ed-ad16-48ae-bdd4-478a94b6aea1"
directoryID = "983a9b43-747c-416a-bfa2-729b3d95a476"
dbrickappdlSec = "AQ~-bKY7.-bMNS9tkekQJIJ06suTx..5C~"
storageResource ="sncprdsnowadls"
directoryName = "Submissions"
mountNameSubmissions = "SubmissionsMnt"
directoryNameSites = "ReferenceData"
mountNameReferenceData = "ReferenceData"
directoryNameSnowEvent = "SnowEvent"
mountNameSnowEvent = "SnowEvent"
fileSystemName = "snow"
dttr="'HOUR'"
maxDate = "2030-01-01 01:00:00.000"


# COMMAND ----------

configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
           "fs.adl.oauth2.client.id": applicationID,
           "fs.adl.oauth2.credential": dbrickappdlSec,
           "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/"+directoryID+"/oauth2/token"}

# COMMAND ----------

#dbutils.fs.unmount("/mnt/"+mountNameSnowEvent)
if any(mount.mountPoint == "/mnt/"+mountNameSnowEvent for mount in dbutils.fs.mounts()):
  print('already mounted')
else :
  dbutils.fs.mount(
  source = "adl://"+storageResource+".azuredatalakestore.net/" + directoryNameSnowEvent,
  mount_point = "/mnt/"+mountNameSnowEvent ,
  extra_configs = configs)
#%fs ls "dbfs:/mnt/SnowEvent" 

# COMMAND ----------

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