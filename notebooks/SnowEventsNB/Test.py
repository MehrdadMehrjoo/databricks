# Databricks notebook source
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



from datetime import datetime, date , timedelta
t1="2019-10-29 18:00:00"
t2="2019-11-03 05:00:00"
t3 = StrToDate(t1) + timedelta(hours=1)
sDate = StrToDate(t1)
eDate = StrToDate(t2)
while sDate < eDate:
  print(sDate)
  dbutils.notebook.run("SnowEvent",120000,{"startdateparam":sDate,"enddateparam":sDate + timedelta(hours=1)}) 
  sDate = sDate + timedelta(hours=1)
  

  


# COMMAND ----------

 