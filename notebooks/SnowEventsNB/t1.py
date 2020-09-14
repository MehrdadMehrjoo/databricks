# Databricks notebook source
# MAGIC %sql
# MAGIC Select * from snowEvent where fsa= "T3G"

# COMMAND ----------

# MAGIC %sql
# MAGIC --Select FSA,LDU,count(*) from snowEvent group by FSA,LDU having count(*) > 3
# MAGIC --Select * from snowEvent where FSA="T4A" order by ifnull(NextEventId,999)
# MAGIC Select * from snowEventDetails where FSA ="T3G" --and submissoinDate is null

# COMMAND ----------

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
print ("Start")
print (processFromDate)
print (processToDate)

# COMMAND ----------

print("B")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select  *   from snowEventDetails where FSA = "T2A" and id=3 and submissionDate is not null --order by ifnull(NextEventid,99999)

# COMMAND ----------

from datetime import datetime, date , timedelta
n = 0 

def UtcDate(day):
    now =  datetime.utcnow() + timedelta(days=day)
    return now.strftime('%Y/%m/%d')
    
def fileExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

def getLatestSites():
  global n 
  if(fileExists("/mnt/ReferenceData/DailyUsqlTable/" + UtcDate(n) + "/dbo.SitesAll_Usql")):
        return "/mnt/ReferenceData/DailyUsqlTable/" + UtcDate(n) + "/dbo.SitesAll_Usql"
  else:
        n = n-1
        return getLatestSites()


latestSitePath =getLatestSites()
qry = 'CREATE OR REPLACE TEMPORARY VIEW sites (Id STRING ,Province STRING ,City STRING ,Region STRING ,Address STRING ,Fsa STRING ,Location STRING ,Lat STRING ,Long STRING ,ProcessedTime STRING ,Ldu STRING ,SnowResponsibility STRING ,Active STRING ,StartDate STRING ,EndDate STRING) USING CSV OPTIONS (path "{}", header "false", mode "FAILFAST",sep "	")'.format(latestSitePath)
#print(qry)
sqlContext.sql(qry)


# COMMAND ----------

snowYear=2020
mountNameSubmissions = "SubmissionsMnt"
submissionAllInputPath = "dbfs:/mnt/"+mountNameSubmissions+"/HourlyRaw/SnowYear"+str(snowYear)+"/"

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsAll (id STRING ,CorrelationID STRING,SiteName STRING,fsa STRING,status STRING,latitude STRING,longitude STRING,siteface STRING,city STRING,Province STRING,region STRING,Address STRING,datetaken STRING,datetakenUTC STRING,CellNumber STRING,DeviceID STRING,UserName STRING,picture STRING,eventenqueuedutctime STRING,EventProcessedUtcTime STRING,PartitionId STRING,BlobName STRING,BlobLastModifiedUtcTime STRING,Accuracy STRING,UserSiteName STRING,PLatitude STRING,PLongitude STRING) USING CSV OPTIONS (path "{}" , header "false", mode "FAILFAST",sep "|")'.format(submissionAllInputPath+"*.txt")
print(qry)
sqlContext.sql(qry)


# COMMAND ----------

# MAGIC %sql
# MAGIC select  * from submissionsAll where datetakenUTC >= '2019-09-28 17:00:00' and datetakenUTC<='2019-09-28 18:00:00' and
# MAGIC  fsa = "T2X"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sites  where ltrim(rtrim(fsa)) = "T2X" 

# COMMAND ----------

inDatetakenDH = "2019-09-28 17:00:00"
maxDate = "2030-01-01 01:00:00.000"
inFSA="T2X"
inLDU=""
InsertQueryDetail = 'Select * from sites Where 	cast(sites.StartDate as TIMESTAMP) <= cast("{0}" as TIMESTAMP) AND cast("{0}" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{1}") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "{2}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{3}" '.format(inDatetakenDH , maxDate,inFSA,inLDU)
    
print(InsertQueryDetail)

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from sites Where 	cast(sites.StartDate as TIMESTAMP) <= cast("2019-09-28 17:00:00" as TIMESTAMP) AND cast("2019-09-28 17:00:00" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"2030-01-01 01:00:00.000") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "T2X" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="" 

# COMMAND ----------

inFSA ="T2A"
inLDU=""
inDatetakenDH="2019-10-03 20:00:00"
inDatetakenDH="2019-10-03 20:00:00"
maxDate = "2030-01-01 01:00:00.000"

qry = 'Select ifnull(min(Region),"") as Region ,  count(*) as TotalLocation from sites 	Where	ltrim(rtrim(FSA)) = "{}" and (Case when Substring(ltrim(rtrim(FSA)),2,1) = "0" Then  ifnull(LDU,"") else "" end) = "{}"  AND cast(StartDate as TIMESTAMP) <= cast("{}" as TIMESTAMP) AND cast("{}" as TIMESTAMP) < cast(ifnull(EndDate ,"{}") as TIMESTAMP)'.format( inFSA,inLDU,inDatetakenDH,inDatetakenDH,maxDate)

df= sqlContext.sql(qry)
result_pdf = df.select("*").toPandas()
print(result_pdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEvent2
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,StartDate TIMESTAMP,EndDate TIMESTAMP,TotalLocationCount int ,LocationCount int ,SubmissionCount int ,CalculatinDate TIMESTAMP,LastSubmissionDate TIMESTAMP,NextEventId int,NextEventDate TIMESTAMP,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING , Action STRING) 
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEventDetails2
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,Province STRING ,City STRING ,Address STRING ,Location STRING ,Lat STRING ,Long STRING ,SnowResponsibility STRING ,SubmissionDate TIMESTAMP) 
# MAGIC USING delta

# COMMAND ----------


snowEventCsv = "/mnt/SnowEvent/2020/SnowEvent.csv"
qry = 'CREATE OR REPLACE TEMPORARY VIEW snowEventTempCSV (rw STRING,SnowYear STRING,id STRING ,Region STRING,FSA STRING,LDU STRING,StartDate STRING,EndDate STRING,TotalLocationCount STRING ,LocationCount STRING ,SubmissionCount STRING ,CalculatinDate STRING,LastSubmissionDate STRING,NextEventId STRING,NextEventDate STRING,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING,Action STRING) USING CSV OPTIONS (path "{}", header "true", mode "FAILFAST",sep ",")'.format(snowEventCsv)
#print("dbfs:" + snowEventCsv)
print(qry)
#sqlContext.sql(qry)
df= sqlContext.sql(qry)
result_pdf = df.select("*").toPandas()
print(result_pdf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from snowEventTempCSV

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into SnowEvent2 select SnowYear ,id  ,Region ,FSA ,LDU ,StartDate ,EndDate ,TotalLocationCount  ,LocationCount  ,SubmissionCount  ,CalculatinDate ,LastSubmissionDate ,NextEventId ,NextEventDate ,Status ,SubmissionFile ,sitesFile , Vendor  , VendorName ,Action  from snowEventTempCSV

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from SnowEvent2

# COMMAND ----------

snowEventParq="/mnt/SnowEvent/2020/SnowEventDetails.parquet"
df= sqlContext.sql("Select * from SnowEventDetails")
df.write.option("compression", "snappy").mode("overwrite").parquet(snowEventParq)  
#result_pdf = df.select("*").toPandas()
#result_pdf.to_csv("/dbfs"+snowEventCsv)

# COMMAND ----------

snowEventParq="/mnt/SnowEvent/2020/SnowEventDetails.parquet"
try:
  qry = 'CREATE OR REPLACE TEMPORARY VIEW SnowEventDetailsTemp USING Parquet OPTIONS (path "{}")'.format("dbfs:" + snowEventParq)
  sqlContext.sql(qry)
except:
  print('No file')

# COMMAND ----------

# MAGIC %sql 
# MAGIC --select * from SnowEventDetailsTemp
# MAGIC --Insert into SnowEventDetails2 select * from SnowEventDetailsTemp
# MAGIC --select count(*) from SnowEventDetails2

# COMMAND ----------

