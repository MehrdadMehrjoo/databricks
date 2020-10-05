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
submissionAllInputPath = "dbfs:/mnt/"+mountNameSubmissions+"/HourlyRaw/SnowYear2020/" #*.txt


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
#df= sqlContext.sql("Select * from snowEvent")
#df.write.option("compression", "snappy").mode("overwrite").parquet(snowEventParq)  
try:
  qry = 'CREATE OR REPLACE TEMPORARY VIEW snowEventTemp1 USING Parquet OPTIONS (path "{}")'.format("dbfs:" + snowEventParq)
  #print(qry)
  sqlContext.sql(qry)
except:
  print('No file')


# COMMAND ----------

# MAGIC %sql
# MAGIC Drop TABLE SnowEvent

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEvent
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,StartDate TIMESTAMP,EndDate TIMESTAMP,TotalLocationCount int ,LocationCount int ,SubmissionCount int ,CalculatinDate TIMESTAMP,LastSubmissionDate TIMESTAMP,NextEventId int,NextEventDate TIMESTAMP,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING , Action STRING) 
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into SnowEvent select * from snowEventTemp1

# COMMAND ----------

#days = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"]
days = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10"]
for dd in days:
  submissionInputPath =  "dbfs:/mnt/SubmissionsMnt/HourlyRaw/2019/11/" + dd+ "/"
  submissionAllInputPath = "dbfs:/mnt/SubmissionsMnt/HourlyRaw/SnowYear2020/"
  dbutils.fs.cp(submissionInputPath,submissionAllInputPath,True)


# COMMAND ----------

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsAll (id STRING ,CorrelationID STRING,SiteName STRING,fsa STRING,status STRING,latitude STRING,longitude STRING,siteface STRING,city STRING,Province STRING,region STRING,Address STRING,datetaken STRING,datetakenUTC STRING,CellNumber STRING,DeviceID STRING,UserName STRING,picture STRING,eventenqueuedutctime STRING,EventProcessedUtcTime STRING,PartitionId STRING,BlobName STRING,BlobLastModifiedUtcTime STRING,Accuracy STRING,UserSiteName STRING,PLatitude STRING,PLongitude STRING,siteFSA STRING,siteLDU STRING) USING CSV OPTIONS (path "{}" , header "false", mode "FAILFAST",sep "|")'.format(submissionAllInputPath+"*.txt")
print(qry)
sqlContext.sql(qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS AllSubmissions
# MAGIC (id STRING ,CorrelationID STRING,SiteName STRING,fsa STRING,status STRING,latitude STRING,longitude STRING,siteface STRING,city STRING,Province STRING,region STRING,Address STRING,datetaken TIMESTAMP,datetakenUTC TIMESTAMP,CellNumber STRING,DeviceID STRING,UserName STRING,picture STRING,eventenqueuedutctime TIMESTAMP,EventProcessedUtcTime TIMESTAMP,PartitionId STRING,BlobName STRING,BlobLastModifiedUtcTime STRING,Accuracy STRING,UserSiteName STRING,PLatitude STRING,PLongitude STRING,siteFSA STRING,siteLDU STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql 
# MAGIC Insert into AllSubmissions select * from submissionsAll

# COMMAND ----------

