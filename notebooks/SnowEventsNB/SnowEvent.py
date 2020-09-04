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
print ("Start")
print (processFromDate)
print (processToDate)


# COMMAND ----------

from datetime import datetime, date , timedelta

def UtcNow():
    now = datetime.utcnow()
    return now
  
def StrToDate(instr):
  if(len(instr)>19):
    instr = instr[0:19]
  return datetime.strptime(instr,'%Y-%m-%d %H:%M:%S')
  

def UtcDate(day):
    now =  datetime.utcnow() + timedelta(days=day)
    return now.strftime('%Y/%m/%d')
  

# COMMAND ----------

def fileExists(path):
  try:
    dbutils.fs.ls(path)
    return True
  except Exception as e:
    if 'java.io.FileNotFoundException' in str(e):
      return False
    else:
      raise

# COMMAND ----------

def getSnowYear(inDate):
  if(StrToDate(inDate).month >=8):
    return StrToDate(inDate).year+1
  else:
    return StrToDate(inDate).year

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

ReferenceDataFolderName ="/mnt/" +mountNameReferenceData + "/DailyUsqlTable/" ;



# COMMAND ----------

n = 0 
def getLatestSites():
  global n 
  if(fileExists(ReferenceDataFolderName + UtcDate(n) + "/dbo.SitesAll_Usql")):
        return ReferenceDataFolderName + UtcDate(n) + "/dbo.SitesAll_Usql"
  else:
        n = n-1
        return getLatestSites()
      

# COMMAND ----------

nv = 0 
def getLatestFSAVendor():
  global nv 
  if(fileExists(ReferenceDataFolderName + UtcDate(n) + "/dbo.FSAVendorAll_Usql")):
        return ReferenceDataFolderName + UtcDate(n) + "/dbo.FSAVendorAll_Usql"
  else:
        nv = nv-1
        return getLatestFSAVendor()

# COMMAND ----------

#Copy submission to root of snow year
print(submissionInputPath)
print(submissionAllInputPath)
dbutils.fs.cp(submissionInputPath,submissionAllInputPath)

# COMMAND ----------

configs = {"fs.adl.oauth2.access.token.provider.type": "ClientCredential",
           "fs.adl.oauth2.client.id": applicationID,
           "fs.adl.oauth2.credential": dbrickappdlSec,
           "fs.adl.oauth2.refresh.url": "https://login.microsoftonline.com/"+directoryID+"/oauth2/token"}

# COMMAND ----------

if any(mount.mountPoint == "/mnt/"+mountNameSubmissions for mount in dbutils.fs.mounts()):
  print('already mounted')
else :
  dbutils.fs.mount(
  source = "adl://"+storageResource+".azuredatalakestore.net/" + directoryName,
  mount_point = "/mnt/"+mountNameSubmissions ,
  extra_configs = configs)
  
  ##%fs ls "dbfs:/mnt/SubmissionsMnt/"

# COMMAND ----------

#dbutils.fs.unmount("/mnt/"+mountNameSites)
if any(mount.mountPoint == "/mnt/"+mountNameReferenceData for mount in dbutils.fs.mounts()):
  print('already mounted')
else :
  dbutils.fs.mount(
  source = "adl://"+storageResource+".azuredatalakestore.net/" + directoryNameSites,
  mount_point = "/mnt/"+mountNameReferenceData ,
  extra_configs = configs)
  #%fs ls "dbfs:/mnt/ReferenceData/DailyUsqlTable/2020/04/01"

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

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEvent
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,StartDate TIMESTAMP,EndDate TIMESTAMP,TotalLocationCount int ,LocationCount int ,SubmissionCount int ,CalculatinDate TIMESTAMP,LastSubmissionDate TIMESTAMP,NextEventId int,NextEventDate TIMESTAMP,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING , Action STRING) 
# MAGIC USING delta
# MAGIC --PARTITIONED BY (StartDate)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE  IF NOT EXISTS SnowEventDetails
# MAGIC (SnowYear int ,id int , Region STRING ,FSA STRING,LDU STRING,Province STRING ,City STRING ,Address STRING ,Location STRING ,Lat STRING ,Long STRING ,SnowResponsibility STRING ,SubmissionDate TIMESTAMP) 
# MAGIC USING delta

# COMMAND ----------

#  try:
#    qry = 'CREATE OR REPLACE TEMPORARY VIEW snowEventTemp USING Parquet OPTIONS (path "{}")'.format("dbfs:" + snowEventParq)
#    #print(qry)
#    sqlContext.sql(qry)
#  except:
#    print('No file')
  

# COMMAND ----------

#qry = 'CREATE OR REPLACE TEMPORARY VIEW snowEventTempCSV (rw STRING,SnowYear int,id int ,Region STRING,FSA STRING,LDU STRING,StartDate TIMESTAMP,EndDate TIMESTAMP,TotalLocationCount int ,LocationCount int ,SubmissionCount int ,CalculatinDate TIMESTAMP,LastSubmissionDate TIMESTAMP,NextEventId int,NextEventDate TIMESTAMP,Status STRING,SubmissionFile STRING,sitesFile STRING, Vendor STRING , VendorName STRING,Action STRING) USING CSV OPTIONS (path "{}", header "true", mode "FAILFAST",sep ",")'.format(snowEventCsv)
#print("dbfs:" + snowEventCsv)
#print(qry)
#sqlContext.sql(qry)


# COMMAND ----------

#sqlContext.sql("Truncate table SnowEvent")
#sqlContext.sql("Truncate table SnowEventDetails")


# COMMAND ----------

#try:
#    #qry = 'insert into SnowEvent Select * from snowEventTemp'
#    qry = 'insert into SnowEvent Select SnowYear,id ,Region,FSA ,LDU ,StartDate ,EndDate ,TotalLocationCount ,LocationCount  ,SubmissionCount ,CalculatinDate ,LastSubmissionDate ,NextEventId ,NextEventDate ,Status ,SubmissionFile,sitesFile,Vendor,VendorName,Action from snowEventTempCSV'
#    sqlContext.sql(qry)
#except:
#    print('No data')

# COMMAND ----------

#qry = 'UPDATE SnowEvent SET ENDDate = (LastSubmissionDate + INTERVAL 24 HOURS) , Action= "{0}" where ENDDate is not null AND (LastSubmissionDate + INTERVAL 24 HOURS) <= "{1}"'.format('Update EndDate',processFromDate)
qry = 'UPDATE SnowEvent SET ENDDate = (LastSubmissionDate + INTERVAL 24 HOURS)  where ENDDate is null AND (LastSubmissionDate + INTERVAL 24 HOURS) <= "{0}"'.format(processFromDate)
#print(qry)
sqlContext.sql(qry)

# COMMAND ----------

latestSitePath =getLatestSites()
qry = 'CREATE OR REPLACE TEMPORARY VIEW sites (Id STRING ,Province STRING ,City STRING ,Region STRING ,Address STRING ,Fsa STRING ,Location STRING ,Lat STRING ,Long STRING ,ProcessedTime STRING ,Ldu STRING ,SnowResponsibility STRING ,Active STRING ,StartDate STRING ,EndDate STRING) USING CSV OPTIONS (path "{}", header "false", mode "FAILFAST",sep "	")'.format(latestSitePath)
#print(qry)
sqlContext.sql(qry)

# COMMAND ----------

latestFSAVendorPath =getLatestFSAVendor()
qry = 'CREATE OR REPLACE TEMPORARY VIEW FSAVendor (FSA STRING ,LDU STRING ,Vendor STRING ,VendorName STRING ,StartDate STRING ,EndDate STRING) USING CSV OPTIONS (path "{}", header "false", mode "FAILFAST",sep "	")'.format(latestFSAVendorPath)
#print(qry)
sqlContext.sql(qry)

# COMMAND ----------

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissions (id STRING ,CorrelationID STRING,SiteName STRING,fsa STRING,status STRING,latitude STRING,longitude STRING,siteface STRING,city STRING,Province STRING,region STRING,Address STRING,datetaken STRING,datetakenUTC STRING,CellNumber STRING,DeviceID STRING,UserName STRING,picture STRING,eventenqueuedutctime STRING,EventProcessedUtcTime STRING,PartitionId STRING,BlobName STRING,BlobLastModifiedUtcTime STRING,Accuracy STRING,UserSiteName STRING,PLatitude STRING,PLongitude STRING) USING CSV OPTIONS (path "{}" , header "false", mode "FAILFAST",sep "|")'.format(submissionInputPath)
#print(qry)
sqlContext.sql(qry)

# COMMAND ----------

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsSnowYear As SELECT * from ( Select distinct * from submissions where  datetakenutc is not null) as ss where  replace((left(ss.EventProcessedUtcTime,19)),"T", " ") >= "{0}" and replace((left(ss.EventProcessedUtcTime,19)),"T", " ") < "{1}" AND ss.datetakenutc >= "{2}" and ss.datetakenutc <= "{3}"'.format(processFromDate,processToDate,snowYearStart,snowYearEnd)
print(qry)
sqlContext.sql(qry)

# COMMAND ----------

#qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsProcessHour As SELECT SiteName, date_trunc({0},datetakenutc) as datetakenDH ,lastSubmission, sum(SubmissionCount) as SubmissionCount , sum(LocationCount) as LocationCount from  (   SELECT SiteName, datetakenutc , count(*) SubmissionCount , 1 LocationCount  , max(datetakenutc) as lastSubmission FROM submissionsSnowYear  group by SiteName, datetakenutc   ) as aa  group by SiteName,date_trunc({0},datetakenutc),lastSubmission'.format(dttr) 
#print(qry)
#sqlContext.sql(qry)

# COMMAND ----------

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsProcessHour As SELECT SiteName, date_trunc({0},datetakenutc) as datetakenDH ,max(datetakenutc) as lastSubmission, count(SiteName) as SubmissionCount  , 1 LocationCount from  submissionsSnowYear   group by SiteName,date_trunc({0},datetakenutc)'.format(dttr) 
print(qry)
sqlContext.sql(qry)

# COMMAND ----------

qry = 'CREATE  OR REPLACE  TEMPORARY VIEW submissionsProcessHourFSA As  Select * from (select sites.Region ,sites.FSA , Case when Substring(sites.FSA,2,1) = "0" Then  ifnull(sites.LDU,"") else "" end AS LDU, datetakenDH,      max(lastSubmission) as lastSubmission,       sum(SubmissionCount) as SubmissionCount,       sum(LocationCount) as LocationCount from submissionsProcessHour inner join sites on sites.Location = submissionsProcessHour.sitename  	Where 	cast(sites.StartDate as TIMESTAMP) <= cast(datetakenDH as TIMESTAMP) AND cast(datetakenDH as TIMESTAMP) <  cast(ifnull(sites.EndDate ,"{0}") as TIMESTAMP) group by  sites.Region ,sites.FSA ,Case when Substring(sites.FSA,2,1) = "0" Then  ifnull(sites.LDU,"") else "" end ,datetakenDH ) as aa order by aa.Region , aa.FSA , aa.LDU,aa.datetakenDH'.format(maxDate)
print(qry)
sqlContext.sql(qry)


# COMMAND ----------

def getTotalLocation(inFSA,inLDU,inDatetakenDH):
  qry = 'Select ifnull(min(Region),"") as Region ,  count(*) as TotalLocation from sites 	Where	ltrim(rtrim(FSA)) = "{}" and (Case when Substring(ltrim(rtrim(FSA)),2,1) = "0" Then  ifnull(LDU,"") else "" end) = "{}"  AND cast(StartDate as TIMESTAMP) <= cast("{}" as TIMESTAMP) AND cast("{}" as TIMESTAMP) < cast(ifnull(EndDate ,"{}") as TIMESTAMP)'.format( inFSA,inLDU,inDatetakenDH,inDatetakenDH,maxDate)
  df= sqlContext.sql(qry)
  result_pdf = df.select("*").toPandas()
  return result_pdf.iloc[0]["TotalLocation"] , result_pdf.iloc[0]["Region"]

# COMMAND ----------

def getVendor(inFSA,inLDU,inDatetakenDH):
  vendorId="0" 
  vendorName="Unassigned"
  qry = 'Select * from FSAVendor Where	ltrim(rtrim(FSA)) = "{}" and ltrim(rtrim(ifnull(LDU,"")))="{}"  AND cast(StartDate as TIMESTAMP) <= cast("{}" as TIMESTAMP) AND cast("{}" as TIMESTAMP) <  cast(ifnull(EndDate ,"{}") as TIMESTAMP)'.format(inFSA.strip(),inLDU[0:2],inDatetakenDH,inDatetakenDH,maxDate)
  df= sqlContext.sql(qry)
  number_of_rows = df.count()
  if(number_of_rows >= 1):
    result_pdf = df.select("*").toPandas()
    vendorId = result_pdf.iloc[0]["Vendor"]
    vendorName = result_pdf.iloc[0]["VendorName"]
  else:
    qry2 = 'Select * from FSAVendor Where	ltrim(rtrim(FSA)) = "{}" AND cast(StartDate as TIMESTAMP) <= cast("{}" as TIMESTAMP) AND cast("{}" as TIMESTAMP) < cast(ifnull(EndDate ,"{}" ) as TIMESTAMP)'.format(inFSA.strip(),inDatetakenDH,inDatetakenDH,maxDate)
    df2= sqlContext.sql(qry2)
    number_of_rows2 = df2.count()
    if(number_of_rows2 >= 1):
      result_pdf2 = df2.select("*").toPandas()
      vendorId = result_pdf2.iloc[0]["Vendor"]
      vendorName = result_pdf2.iloc[0]["VendorName"]
    
  return vendorId , vendorName
  


# COMMAND ----------

def InsertSnowEvent(inSnowYear ,inRegion,inFSA,inLDU,inDatetakenDH,SubmissionCount,LocationCount ,lastSubmission,nextEventId,nextEventDate, action):
  totalLocation,Region = getTotalLocation(inFSA,inLDU,inDatetakenDH);
  vendor,vendorName = getVendor(inFSA,inLDU,inDatetakenDH);
  
  maxIdQuery = 'Select ifnull(max(id),0) as  maxId from snowEvent where SnowYear={} and ltrim(rtrim(Region))="{}" and FSA="{}" and ifnull(LDU,"") ="{}"'.format(inSnowYear , inRegion, inFSA,inLDU)
  print("getMax")
  print(maxIdQuery)
  df = sqlContext.sql(maxIdQuery)
  result_pdf = df.select("maxId").toPandas()
  maxId = result_pdf.iloc[0][0] +1 
  
  SubmissionFile =  inDatetakenDH.strftime('%Y_%m_%d_%H_%M') + "_" + inRegion+inFSA + inLDU +"_"+str(maxId)+".csv"
  sitesFile =  "site" + inDatetakenDH.strftime('%Y_%m_%d_%H_%M') + "_" + inRegion +inFSA + inLDU +"_"+str(maxId)+".csv"
  
  if(nextEventId == "null") :
    InsertQuery = 'INSERT INTO TABLE snowEvent  Select {}, {},"{}","{}","{}","{}",null,{},{},{},"{}","{}",null,null,"","{}","{}","{}","{}","{}" '.format( inSnowYear,maxId,Region,inFSA,inLDU,inDatetakenDH,totalLocation,LocationCount ,SubmissionCount,UtcNow(),lastSubmission,SubmissionFile,sitesFile ,vendor,vendorName,action) #nextEventId,nextEventDate,
  else :
    endDate = StrToDate(lastSubmission)+ timedelta(hours=24)
    InsertQuery = 'INSERT INTO TABLE snowEvent  Select {},{},"{}","{}","{}","{}","{}",{},{},{},"{}","{}",{},"{}","","{}","{}","{}","{}","{}" '.format(inSnowYear,maxId,Region,inFSA,inLDU,inDatetakenDH,endDate,totalLocation, LocationCount ,SubmissionCount, UtcNow(),lastSubmission ,nextEventId, nextEventDate ,SubmissionFile , sitesFile ,vendor,vendorName,action)
  
  print(InsertQuery)
  sqlContext.sql(InsertQuery)
  
  InsertQueryDetail = 'INSERT INTO TABLE SnowEventDetails Select {0},{1},"{2}","{3}","{4}", sites.Province, sites.City,Address,Location,Lat,Long,SnowResponsibility,ss.lastDatetakenUTC from sites left join (select sitename,max(datetakenUTC) as lastDatetakenUTC  from submissions where datetakenUTC>="{5}" AND datetakenUTC<= "{6}" group by sitename) as ss on sites.Location = ss.sitename  	Where 	cast(sites.StartDate as TIMESTAMP) <= cast("{5}" as TIMESTAMP) AND cast("{5}" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{7}") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "{3}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{4}" '.format(inSnowYear,maxId,inRegion,inFSA,inLDU,inDatetakenDH ,lastSubmission, maxDate)
    
  print(InsertQueryDetail)
  sqlContext.sql(InsertQueryDetail)
  
  
  
  submissionQryIns = 'Select "{}" as SnowYear ,"{}" as SnowEventId, submissions.* from submissions inner join sites on sites.Location = submissions.sitename  	Where cast(sites.StartDate as TIMESTAMP) <=  cast(datetakenUTC as TIMESTAMP) AND cast(datetakenUTC as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{}") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "{}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{}" AND cast(datetakenUTC as TIMESTAMP)>= cast("{}" as TIMESTAMP) AND cast(datetakenUTC as TIMESTAMP)<= cast("{}" as TIMESTAMP)'.format(str(inSnowYear) ,maxId,maxDate,inFSA,inLDU,inDatetakenDH,lastSubmission)
  dfSub = sqlContext.sql(submissionQryIns)
  print(submissionQryIns)
  result_pdf = dfSub.select("*").toPandas()
  result_pdf.to_csv("/dbfs"+snowEventSubmission + SubmissionFile)
  
  siteQryIns = 'Select {0},{1},"{2}","{3}","{4}", sites.Province, sites.City,Address,Location,Lat,Long,SnowResponsibility,ss.lastDatetakenUTC from sites left join (select sitename,max(datetakenUTC) as lastDatetakenUTC  from submissions where datetakenUTC>="{5}" AND datetakenUTC<= "{6}" group by sitename) as ss on sites.Location = ss.sitename  	Where 	sites.StartDate <= "{5}" AND "{5}" < ifnull(sites.EndDate ,"{7}")  AND ltrim(rtrim(sites.FSA)) = "{3}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{4}" '.format(inSnowYear,maxId,inRegion,inFSA,inLDU,inDatetakenDH ,lastSubmission, maxDate)
  dfSubSite = sqlContext.sql(siteQryIns)
  print(siteQryIns)
  result_pdfSite = dfSubSite.select("*").toPandas()
  result_pdfSite.to_csv("/dbfs"+snowEventSite + sitesFile)
  
  
  return  maxId

# COMMAND ----------

qry = 'CREATE  OR REPLACE TEMPORARY VIEW submissionsAll (id STRING ,CorrelationID STRING,SiteName STRING,fsa STRING,status STRING,latitude STRING,longitude STRING,siteface STRING,city STRING,Province STRING,region STRING,Address STRING,datetaken STRING,datetakenUTC STRING,CellNumber STRING,DeviceID STRING,UserName STRING,picture STRING,eventenqueuedutctime STRING,EventProcessedUtcTime STRING,PartitionId STRING,BlobName STRING,BlobLastModifiedUtcTime STRING,Accuracy STRING,UserSiteName STRING,PLatitude STRING,PLongitude STRING) USING CSV OPTIONS (path "{}" , header "false", mode "FAILFAST",sep "|")'.format(submissionAllInputPath+"*.txt")
print(qry)
sqlContext.sql(qry)

# COMMAND ----------

def setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion):
  lastSubmissionDateStr = str(lastSubmissionDate)
  if(len(str(lastSubmissionDate)) == 19 ) :
    lastSubmissionDateStr = str(lastSubmissionDate) + '.999'
    
  qry = 'CREATE  OR REPLACE  TEMPORARY VIEW submissionsSum As   Select distinct * from submissionsAll WHERE datetakenutc >= "{0}" and datetakenutc <= "{1}"     and datetakenutc is not null '.format(startDate,lastSubmissionDateStr)
  print(qry)
  sqlContext.sql(qry)

  sumquery = ' SELECT sum(SubmissionCount) as SubmissionCount, sum(LocationCount) as LocationCount from ( SELECT SiteName,ltrim(rtrim(sites.FSA)) as FSA,Case when Substring(sites.FSA,2,1) = "0" Then  ifnull(sites.LDU,"") else "" end as LDU, count(*) as SubmissionCount , 1 as LocationCount  FROM submissionsSum inner join sites on sites.Location = submissionsSum.sitename  	Where   cast(sites.StartDate as TIMESTAMP) <= cast("{0}" as TIMESTAMP) AND 	cast("{0}" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{1}" ) as TIMESTAMP)  and ltrim(rtrim(sites.FSA)) ="{2}" and Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end = "{3}"   group by SiteName ,ltrim(rtrim(sites.FSA)),Case when Substring(sites.FSA,2,1) = "0" Then  ifnull(sites.LDU,"") else "" end )'.format(startDate,maxDate, inFSA,inLDU)
  print(sumquery)
  df = sqlContext.sql(sumquery)
  result_pdf = df.select("*").toPandas()
  SubmissionCount = result_pdf.iloc[0]["SubmissionCount"]
  LocationCount = result_pdf.iloc[0]["LocationCount"]
  print(SubmissionCount)
  
  
  DeleteQueryDetail = 'Delete from SnowEventDetails Where SnowYear={0} AND id ={1} AND ltrim(rtrim(Region))="{2}" and ltrim(rtrim(FSA)) = "{3}" AND (Case when Substring(ltrim(rtrim(FSA)),2,1) = "0" Then  ifnull(LDU,"") else "" end) ="{4}" '.format(inSnowYear,snowId,inRegion,inFSA,inLDU)
    
  print(DeleteQueryDetail)
  sqlContext.sql(DeleteQueryDetail)
  print("---------")
  InsertQueryDetail = 'INSERT INTO TABLE SnowEventDetails Select {0},{1},"{2}","{3}","{4}", sites.Province, sites.City,Address,Location,Lat,Long,SnowResponsibility,ss.lastDatetakenUTC from sites left join (select sitename,max(datetakenUTC) as lastDatetakenUTC  from submissionsSum where  group by sitename) as ss on sites.Location = ss.sitename  	Where 	cast(sites.StartDate as TIMESTAMP)<= cast("{5}" as TIMESTAMP)  AND cast("{5}" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{6}") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "{3}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{4}" '.format(inSnowYear,snowId,inRegion,inFSA,inLDU,startDate , maxDate)
  print('{0}-{1}-{2}-{3}-{4}-{5}-{6}'.format(inSnowYear,snowId,inRegion,inFSA,inLDU,startDate , maxDate))   
  print(InsertQueryDetail)
  sqlContext.sql(InsertQueryDetail)
  
  
  subDetailquery = ' SELECT "{0}" as SnowYear ,"{1}" as SnowEventId, submissionsSum.* FROM submissionsSum inner join sites on sites.Location = submissionsSum.sitename  	Where   cast(sites.StartDate as TIMESTAMP) <= cast("{2}" as TIMESTAMP) AND 	cast("{2}" as TIMESTAMP) < cast(ifnull(sites.EndDate ,"{3}") as TIMESTAMP)  and ltrim(rtrim(sites.FSA)) ="{4}" and Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end = "{5}" '.format(str(inSnowYear), str(snowId), startDate,maxDate, inFSA,inLDU)
  print(subDetailquery)
  
  SubmissionFile =  startDate.strftime('%Y_%m_%d_%H_%M') + "_" + inRegion + inFSA + inLDU +"_"+str(snowId)+".csv"
  dfDetail = sqlContext.sql(subDetailquery)
  result_pdf = dfDetail.select("*").toPandas()
  result_pdf.to_csv("/dbfs"+snowEventSubmission + SubmissionFile)
  
  
  siteQryIns = 'Select {0},{1},"{2}","{3}","{4}", sites.Province, sites.City,Address,Location,Lat,Long,SnowResponsibility,ss.lastDatetakenUTC from sites left join (select sitename,max(datetakenUTC) as lastDatetakenUTC  from submissionsSum  group by sitename) as ss on sites.Location = ss.sitename  	Where 	cast(sites.StartDate as TIMESTAMP) <= cast("{5}" as TIMESTAMP) AND cast("{5}" as TIMESTAMP) <  cast(ifnull(sites.EndDate ,"{6}") as TIMESTAMP)  AND ltrim(rtrim(sites.FSA)) = "{3}" AND (Case when Substring(ltrim(rtrim(sites.FSA)),2,1) = "0" Then  ifnull(sites.LDU,"") else "" end) ="{4}" '.format(inSnowYear,snowId,inRegion,inFSA,inLDU,startDate , maxDate)
  print(siteQryIns)
  siteFile =  "site"+ startDate.strftime('%Y_%m_%d_%H_%M') + "_" + inRegion + inFSA + inLDU +"_"+str(snowId)+".csv"
  dfSubSite = sqlContext.sql(siteQryIns)
  result_pdfSite = dfSubSite.select("*").toPandas()
  result_pdfSite.to_csv("/dbfs"+snowEventSite + siteFile)
  
  return SubmissionCount , LocationCount , SubmissionFile , siteFile

# COMMAND ----------

#from delta.tables import *
#from pyspark.sql.functions import *
def UpdateSnowEvent(actType ,snowId,inSnowYear , inRegion, inFSA,inLDU,startDate,lastSubmissionDate,nextEventId, nextEventDate,endDate,action):
  
  nextEventIdstr=''
  
  if(str(nextEventId) == 'nan') :
    nextEventIdstr='null'
  else:
    nextEventIdstr = str(nextEventId)
  

  nextEventDatestr=''
  if(str(nextEventDate) == 'NaT') :
    nextEventDatestr='null'
  else:
    nextEventDatestr='"{}"'.format(str(nextEventDate))

  endDatestr=''
  if(str(endDate) == 'NaT' or str(endDate) == 'null') :
    endDatestr='null'
  else:
    endDatestr='"{}"'.format(str(endDate))

    
  print(nextEventIdstr +' ' + nextEventDatestr)
  
  totalLocation,Region = getTotalLocation(inFSA,inLDU,startDate);
  #vendor,vendorName = getVendor(inFSA,inLDU,inDatetakenDH);
  
  
  if (actType == "updateCount") :
    SubmissionCount , LocationCount , SubmissionFile , siteFile = setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion)
    updatquery = 'UPDATE SnowEvent SET TotalLocationCount = {0} , LocationCount={1}, SubmissionCount = {2}, CalculatinDate="{3}" , LastSubmissionDate = "{4}" ,SubmissionFile ="{5}" ,SitesFile ="{6}" ,Action = "{7}" WHERE id = {8} AND FSA="{9}" AND ifnull(LDU,"") = "{10}" and SnowYear ={11} and ltrim(rtrim(Region))="{12}"'.format(totalLocation,LocationCount, SubmissionCount, UtcNow(), lastSubmissionDate, SubmissionFile, siteFile, action, snowId, inFSA, inLDU, inSnowYear , inRegion)
    print(updatquery)
    sqlContext.sql(updatquery)
    
  if (actType == "updateCountEndDate") :
    SubmissionCount , LocationCount , SubmissionFile , siteFile = setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion)
    updatquery = 'UPDATE SnowEvent SET TotalLocationCount = {0} , LocationCount={1}, SubmissionCount = {2}, CalculatinDate="{3}" , LastSubmissionDate = "{4}" ,EndDate = {5} ,SubmissionFile ="{6}" ,SitesFile ="{7}" ,Action = "{8}" WHERE id = {9} AND FSA="{10}" AND ifnull(LDU,"") = "{11}" and SnowYear ={12} and ltrim(rtrim(Region))="{13}"'.format(totalLocation,LocationCount, SubmissionCount, UtcNow(), lastSubmissionDate , endDatestr, SubmissionFile, siteFile, action, snowId, inFSA, inLDU, inSnowYear , inRegion)
    print(updatquery)
    sqlContext.sql(updatquery)
    
  if (actType == "updateEndDateAndNextEvent") :
    SubmissionCount , LocationCount , SubmissionFile , siteFile = setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion)
    updatquery = 'UPDATE SnowEvent SET TotalLocationCount = {0} , LocationCount={1}, SubmissionCount = {2}, CalculatinDate="{3}" , LastSubmissionDate = "{4}" ,EndDate = {5} , NextEventId={6}, NextEventDate = {7} ,SubmissionFile = "{8}" ,sitesFile="{9}" ,Action = "{10}" WHERE id = {11} AND FSA="{12}" AND ifnull(LDU,"") = "{13}" and SnowYear ={14} and ltrim(rtrim(Region))="{15}"'.format(totalLocation,LocationCount, SubmissionCount , UtcNow() , lastSubmissionDate , endDatestr , nextEventIdstr , nextEventDatestr,SubmissionFile,siteFile, action,snowId,inFSA,inLDU,inSnowYear , inRegion )
    print(updatquery)
    sqlContext.sql(updatquery)
    
  if (actType == "updateStartDateAndEndDateAndNextEvent") :
    SubmissionCount , LocationCount , SubmissionFile , siteFile = setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion)
    updatquery = 'UPDATE SnowEvent SET StartDate ="{0}" TotalLocationCount = {1} , LocationCount={2}, SubmissionCount = {3}, CalculatinDate="{4}" , LastSubmissionDate = "{5}" ,EndDate = {6} , NextEventId={7}, NextEventDate = {8} ,SubmissionFile ="{9}" ,SitesFile ="{10}" ,Action = "{11}" WHERE id = {12} AND FSA="{13}" AND ifnull(LDU,"") = "{14}" and SnowYear ={15} and ltrim(rtrim(Region))="{16}"'.format(startDate,totalLocation, LocationCount, SubmissionCount ,UtcNow(), lastSubmissionDate, endDatestr , nextEventIdstr , nextEventDatestr,SubmissionFile ,siteFile , action, snowId , inFSA , inLDU,inSnowYear , inRegion )
    print(updatquery)
    sqlContext.sql(updatquery)
    
  if (actType == "updateNextEventDateOnly") :
    updatquery = 'UPDATE SnowEvent SET  NextEventDate = {0} , CalculatinDate="{1}" ,Action ="{2}" WHERE id = {3} AND FSA="{4}" AND ifnull(LDU,"") = "{5}" and SnowYear ={6} and ltrim(rtrim(Region))="{7}"'.format(nextEventDatestr ,UtcNow() , action, snowId , inFSA , inLDU,inSnowYear , inRegion)
    print(updatquery)
    sqlContext.sql(updatquery)
    
  if (actType == "updateStartDateAndCount") :
    SubmissionCount , LocationCount , SubmissionFile , siteFile = setSubmissionSummery(snowId,startDate,lastSubmissionDate,maxDate, inFSA,inLDU,inSnowYear , inRegion)
    updatquery = 'UPDATE SnowEvent SET StartDate ="{0}" , TotalLocationCount = {1} , LocationCount={2}, SubmissionCount = {3}, CalculatinDate="{4}" , LastSubmissionDate = "{5}" , SubmissionFile ="{6}",SitesFile ="{7}" ,Action = "{8}" WHERE id = {9} AND FSA="{10}" AND ifnull(LDU,"") = "{11}" and SnowYear ={12} and ltrim(rtrim(Region))="{13}"'.format( startDate, totalLocation, LocationCount, SubmissionCount ,UtcNow(), lastSubmissionDate, SubmissionFile ,siteFile  , action, snowId , inFSA , inLDU,inSnowYear , inRegion )
    print(updatquery)
    sqlContext.sql(updatquery)
    
    
  if (actType == "Merged") :
    updatquery = 'UPDATE SnowEvent SET Status ="{0}" ,Action = "{1}"  WHERE id = {2} AND FSA="{3}" AND ifnull(LDU,"") = "{4}" and SnowYear ={5} and ltrim(rtrim(Region))="{6}"'.format(actType, action, snowId, inFSA,inLDU,inSnowYear , inRegion )
    print(updatquery)
    sqlContext.sql(updatquery)
    
  
    #deltaTable = DeltaTable.forName(spark,"SnowEvent")
    #deltaTable.update("id = "+snowId, { "LDU": "'ws'" ,"FSA": "'wqw'" } )   # predicate using SQL formatted string

# COMMAND ----------

import pandas as pd
from datetime import datetime, timedelta

def submissionsSnowEvent(inSnowYear , inRegion ,inFSA,inLDU,inDatetakenDH,SubmissionCount,LocationCount,inlastSubmission ):
    selectCmdSnowEvent = 'SELECT * FROM SnowEvent where  SnowYear={} AND  ltrim(rtrim(Region))="{}" AND ltrim(rtrim(FSA))="{}" AND ltrim(rtrim(ifnull(LDU,""))) ="{}" AND ifnull(Status,"") = "" AND  cast(StartDate as TIMESTAMP) <= cast("{}" as TIMESTAMP) AND cast("{}" as TIMESTAMP) < cast((case when ifnull(NextEventDate,"") ="" then "{}" else NextEventDate end) as TIMESTAMP) '.format(inSnowYear , inRegion,inFSA,inLDU,inDatetakenDH,inDatetakenDH,maxDate)
    print(selectCmdSnowEvent)
    df= sqlContext.sql(selectCmdSnowEvent)
    number_of_rows = df.count()
    msg = 'Start submissionsSnowEvent SnowYear={} AND  Region="{}" FSA = "{}" ldu ="{}" Datetaken = "{}" SubmissionCount ="{}" LocationCount ="{}" lastSubmission = "{}"'.format(inSnowYear , inRegion,inFSA,inLDU,inDatetakenDH,SubmissionCount,LocationCount,inlastSubmission)
    print(msg)
    if( number_of_rows > 1 )  :
      print('Error')
    elif(number_of_rows == 0)  :
      selectCmdSnowEvent0 = 'SELECT * FROM SnowEvent where  SnowYear={} AND  ltrim(rtrim(Region))="{}" AND ltrim(rtrim(FSA))="{}" AND ltrim(rtrim(ifnull(LDU,""))) ="{}" AND ifnull(Status,"") = "" '.format(inSnowYear , inRegion,inFSA,inLDU)
      print(selectCmdSnowEvent0)
      df0= sqlContext.sql(selectCmdSnowEvent0)
      number_of_rows0 = df0.count()
      if(number_of_rows0 == 0):
        print('Insert SnowEvent (0)')
        newId = InsertSnowEvent(inSnowYear , inRegion,inFSA, inLDU,inDatetakenDH,SubmissionCount,LocationCount,inlastSubmission,"null","null", "Insert SnowEvent (0)" )
      else:
        result_pdf0 = df0.select("*").toPandas()
        LastSubmissionDate0 = result_pdf0.iloc[0]["LastSubmissionDate"]
        snowId0 = result_pdf0.iloc[0]["id"]
        startDate0 = result_pdf0.iloc[0]["StartDate"]
        if(StrToDate(inlastSubmission) + timedelta(hours=24) < LastSubmissionDate0 ):
          print("Insert SnowEvent (10)")
          newId = InsertSnowEvent(inSnowYear , inRegion, inFSA,inLDU, inDatetakenDH, SubmissionCount, LocationCount, inlastSubmission, snowId0, startDate0 ,"Insert SnowEvent (10)")
        else:
          print("Update SnowEvent (11)")
          UpdateSnowEvent("updateStartDateAndCount",snowId0,inSnowYear , inRegion,inFSA,inLDU, inDatetakenDH , LastSubmissionDate0 ,0,"null","null", "Update SnowEvent (11)")
    else :  
      print('Update_1')
      result_pdf = df.select("*").toPandas()
      snowId = result_pdf.iloc[0]["id"]
      startDate = result_pdf.iloc[0]["StartDate"]
      endDate = result_pdf.iloc[0]["EndDate"]
      lastSubmissionDate = result_pdf.iloc[0]["LastSubmissionDate"] 
      nextEventId = result_pdf.iloc[0]["NextEventId"]
      nextEventDate = result_pdf.iloc[0]["NextEventDate"] 
      if( endDate is pd.NaT ) :
          if (inDatetakenDH <= lastSubmissionDate) :
            print('Insert SnowEvent (1)')
            if (StrToDate(inlastSubmission) > lastSubmissionDate):
              lastSubmissionDate =StrToDate(inlastSubmission)
            UpdateSnowEvent("updateCount",snowId,inSnowYear , inRegion, inFSA,inLDU, startDate, lastSubmissionDate, nextEventId, nextEventDate, "null" , "Update SnowEvent (1)")
          elif (inDatetakenDH <= lastSubmissionDate + timedelta(hours=24) ) :
            print('Insert SnowEvent (2)')
            if (StrToDate(inlastSubmission) > lastSubmissionDate):
              lastSubmissionDate =StrToDate(inlastSubmission)
            UpdateSnowEvent("updateCount",snowId,inSnowYear , inRegion,inFSA, inLDU, startDate, inlastSubmission, nextEventId, nextEventDate, "null", "Update SnowEvent (2)")
          else :
            print('Insert SnowEvent (3)')
            newId = InsertSnowEvent(inSnowYear , inRegion,inFSA,inLDU,inDatetakenDH,SubmissionCount,LocationCount,inlastSubmission ,"null","null","Insert SnowEvent (3)")
            
            if(inDatetakenDH > lastSubmissionDate+ timedelta(hours=24)) :
              endDate = lastSubmissionDate+ timedelta(hours=24)
            else :
              endDate = "null"
            UpdateSnowEvent("updateEndDateAndNextEvent",snowId,inSnowYear , inRegion,inFSA,inLDU, startDate, lastSubmissionDate, newId, inDatetakenDH ,endDate, "Update SnowEvent (3)")
      else:
          if( nextEventDate is pd.NaT ) :
            print('Insert SnowEvent(4)')
            newId = InsertSnowEvent(inSnowYear , inRegion, inFSA,inLDU, inDatetakenDH, SubmissionCount, LocationCount, inlastSubmission, "null", "null" ,"Insert SnowEvent (4)")
            if(inDatetakenDH > lastSubmissionDate+ timedelta(hours=24)) :
              endDate = lastSubmissionDate+ timedelta(hours=24)
            else :
              endDate = "null"
            UpdateSnowEvent("updateEndDateAndNextEvent",snowId,inSnowYear , inRegion, inFSA, inLDU, startDate, lastSubmissionDate, newId, inDatetakenDH, endDate , "Update SnowEvent (4)") #updateNextEventDate
          else:
            if (inDatetakenDH <= lastSubmissionDate) :
              print('Insert SnowEvent(5)')
              #if(inDatetakenDH > lastSubmissionDate+ timedelta(hours=24)) :
              endDate = lastSubmissionDate+ timedelta(hours=24)
              #else :
              #  endDate = "null"
              if (StrToDate(inlastSubmission) > lastSubmissionDate):
                lastSubmissionDate =StrToDate(inlastSubmission)
              UpdateSnowEvent("updateCountEndDate",snowId,inSnowYear , inRegion, inFSA, inLDU, startDate, lastSubmissionDate, nextEventId, nextEventDate, endDate, "Update SnowEvent (5)")
            else:
              if (inDatetakenDH <= endDate) :
                  if (inDatetakenDH + timedelta(hours=24) <= nextEventDate) :
                    print('Insert SnowEvent(6)')
                    if (StrToDate(inlastSubmission) > lastSubmissionDate):
                      lastSubmissionDate = StrToDate(inlastSubmission)
                    #if(inDatetakenDH > lastSubmissionDate+ timedelta(hours=24)) :
                    endDate = lastSubmissionDate+ timedelta(hours=24)
                    #else :
                    #  endDate = "null"
                    UpdateSnowEvent("updateEndDateAndNextEvent",snowId,inSnowYear , inRegion,inFSA,inLDU, startDate, lastSubmissionDate, nextEventId, nextEventDate, endDate , "Update SnowEvent (6)")#updateCountAndEndDate
                  else :
                    print('Insert SnowEvent(7)')
                    selectCmdSnowEventNext = 'SELECT * FROM SnowEvent where ltrim(rtrim(FSA))="{}" AND ltrim(rtrim(ifnull(LDU,""))) ="{}" AND id={} '.format(inFSA,inLDU,nextEventId)
                    print(selectCmdSnowEventNext)
                    dfNxt= sqlContext.sql(selectCmdSnowEventNext)
                    number_of_rowsNxt = dfNxt.count()
                    if(number_of_rowsNxt == 1) :
                      resultNxt_pdf = dfNxt.select("*").toPandas()
                      nextEventLastSubmissionDate = resultNxt_pdf.iloc[0]["LastSubmissionDate"]
                      nextEventNextEventId = resultNxt_pdf.iloc[0]["NextEventId"]
                      print('------nextEventNextEventId-------' + str(nextEventNextEventId))
                      nextEventNextEventDate = resultNxt_pdf.iloc[0]["NextEventDate"]
                      #if(inDatetakenDH > lastSubmissionDate+ timedelta(hours=24)) :
                      endDate = lastSubmissionDate+ timedelta(hours=24)
                      #else :
                      #    endDate = "null"
                      UpdateSnowEvent("updateEndDateAndNextEvent",snowId,inSnowYear , inRegion, inFSA,inLDU, startDate, nextEventLastSubmissionDate, nextEventNextEventId, nextEventNextEventDate,endDate , "Update SnowEvent (7)") #updateCountAndEndDateAndNextEvent
                      UpdateSnowEvent("Merged",nextEventId,inSnowYear , inRegion,inFSA,inLDU, "null", "null","null","null","null" , "Update SnowEvent (Merged)")
                    else:
                      Print("Error2")
              else :
                  if (inDatetakenDH + timedelta(hours=24) <= nextEventDate) :
                    print('Insert SnowEvent(8)')
                    print(nextEventId)
                    print(nextEventDate)
                    newId = InsertSnowEvent(inSnowYear , inRegion, inFSA,inLDU, inDatetakenDH, SubmissionCount, LocationCount, inlastSubmission, nextEventId, nextEventDate ,"Insert SnowEvent (8)")
                    print(endDate)    
                    UpdateSnowEvent("updateEndDateAndNextEvent",snowId,inSnowYear , inRegion,inFSA,inLDU, startDate, lastSubmissionDate, newId, inDatetakenDH, endDate, "Update SnowEvent (8)") #updateNextEventDate
                  else :
                    print('Insert SnowEvent(9)')
                    UpdateSnowEvent("updateNextEventDateOnly",snowId,inSnowYear , inRegion, inFSA,inLDU, startDate, lastSubmissionDate, nextEventId ,inDatetakenDH, endDate,"Update SnowEvent(9)")
                    selectCmdSnowEventNext9 = 'SELECT * FROM SnowEvent where ltrim(rtrim(FSA))="{}" AND ltrim(rtrim(ifnull(LDU,""))) ="{}" AND id={} '.format(inFSA,inLDU,nextEventId)
                    print(selectCmdSnowEventNext9)
                    dfNxt9= sqlContext.sql(selectCmdSnowEventNext9)
                    number_of_rowsNxt9 = dfNxt9.count()
                    if(number_of_rowsNxt9 == 1) :
                      resultNxt_pdf9 = dfNxt9.select("*").toPandas()
                      nextEventLastSubmissionDate9 = resultNxt_pdf9.iloc[0]["LastSubmissionDate"]
                      UpdateSnowEvent("updateStartDateAndCount",nextEventId,inSnowYear , inRegion, inFSA,inLDU, inDatetakenDH , nextEventLastSubmissionDate9 ,0,"null","null", "Update SnowEvent (9)")
                    else:
                      Print("Error3")
                      

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from submissionsProcessHourFSA

# COMMAND ----------

dfsubmissionsProcessHourFSA = spark.table('submissionsProcessHourFSA')
i=0
for f in dfsubmissionsProcessHourFSA.collect():
  curSnowYear = getSnowYear(str(f.datetakenDH))
  submissionsSnowEvent(curSnowYear, f.Region.strip() ,f.FSA.strip(), f.LDU.strip(), f.datetakenDH, f.SubmissionCount, f.LocationCount, f.lastSubmission  )
  #i = i+1
  #if(i ==1) : break

# COMMAND ----------

df= sqlContext.sql("Select * from snowEvent")
#df.coalesce(1).write.format("spark.csv").option("header","true").save("/mnt/SnowEvent/aa.parquet")
#df.write.option("compression", "snappy").mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("/mnt/SnowEvent/aaa.csv")
#df.coalesce(1).write.csv("/mnt/SnowEvent/bbb.csv",header=True) 
df.write.option("compression", "snappy").mode("overwrite").parquet(snowEventParq)  
result_pdf = df.select("*").toPandas()
result_pdf.to_csv("/dbfs"+snowEventCsv)
result_pdf.to_csv("/dbfs"+snowEventCsvLog)

# COMMAND ----------

