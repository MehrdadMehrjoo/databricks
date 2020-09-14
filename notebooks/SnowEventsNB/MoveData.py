# Databricks notebook source
dbutils.widgets.text("startdateparam", "","")
dbutils.widgets.get("startdateparam")
processFromDate = getArgument("startdateparam") #"2019-09-20 00:00:00.000"

dbutils.widgets.text("enddateparam", "","")
dbutils.widgets.get("enddateparam")
processToDate = getArgument("enddateparam") #"2019-10-01 01:00:00.000"

processFromDate = processFromDate.replace("T", " ")
processFromDate = processFromDate.replace("Z", "")
processToDate = processToDate.replace("T", " ")
processToDate = processToDate.replace("Z", "")
print ("Start job devops")
print (processFromDate)
print (processToDate)

# COMMAND ----------

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
submissionInputPath = "dbfs:/mnt/"+mountNameSubmissions+"/HourlyRaw/"+StrToDate(processFromDate).strftime('%Y')+"/" + StrToDate(processFromDate).strftime('%m')+ "/" + StrToDate(processFromDate).strftime('%d')+"/submissions-" + StrToDate(processFromDate).strftime('%Y-%m-%d-%H') +".txt"

snowYear = getSnowYear(str(processFromDate))  

snowYearStart = '{}-08-15 01:00:00.000'.format(str(snowYear-1))
snowYearEnd = '{}-05-30 01:00:00.000'.format(str(snowYear))
  
submissionAllInputPath = "dbfs:/mnt/"+mountNameSubmissions+"/HourlyRaw/SnowYear"+str(snowYear)+"/" #*.txt


snowEventParq = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/SnowEvent.parquet"
snowEventCsv = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/SnowEvent.csv"

snowEventCsvLog = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/Log/SnowEvent"+ StrToDate(processFromDate).strftime('%Y_%m_%d_%H') +"_"+UtcNow().strftime('%Y_%m_%d_%H_%M')  +".csv"
snowEventSubmission = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/Submissions/"
snowEventSite = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/Sites/"


SnowEventDetailsParq = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/SnowEventDetails.parquet"
SnowEventDetailsCsv = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/SnowEventDetails.csv"

SnowEventDetailsCsvLog = "/mnt/" + mountNameSnowEvent+ "/" +str(snowYear) + "/SnowEventDetails/SnowEventDetails"+ StrToDate(processFromDate).strftime('%Y_%m_%d_%H') +"_"+UtcNow().strftime('%Y_%m_%d_%H_%M')  +".csv"

ReferenceDataFolderName ="/mnt/" +mountNameReferenceData + "/DailyUsqlTable/" ;

print(submissionInputPath)

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