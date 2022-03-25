import csv
import json
import twd97
import pandas as pd
from datetime import datetime
from geojson import Feature, Point, FeatureCollection
import os
jsonArray = []
import time
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
import json
def searchfile():
    csv1=[]
    entries = os.listdir('/home/fuxtoy/Desktop/Project/complete_groundwater_data')

    for i in range (0,len(entries)):
        temp=""
        for y in range (len(entries[i])-4,len(entries[i])):
            temp=temp+entries[i][y]
        if temp=='.csv':
            csv1.append(entries[i])
    
    return csv1

    
def updata(file_path):
    jsonArray = []
    with open(r'/home/fuxtoy/Desktop/Project/complete_groundwater_data/{}'.format(file_path), newline='', encoding="UTF-8") as csvfile:
        csvReader = csv.DictReader(csvfile) 
        for row in csvReader:
            #add this python dict to json array
            column=list(row)
            jsonArray.append(row)     
    return jsonArray, column


def changeformat(jsonArray,temp):
    for y in range(0,len(jsonArray)):
        for z in range(0,len(temp)):
            """
            處理需要顯示出來的資料數值 ex 水位高度等...
            """
            if str(temp[z])=='LVDT2':
                jsonArray[y]['{}'.format(temp[z])]=float(jsonArray[y]['{}'.format(temp[z])])
            #elif str(temp[z])=='PTemp':
                #jsonArray[y]['{}'.format(temp[z])]=float(jsonArray[y]['{}'.format(temp[z])])
            #elif str(temp[z])=='batt_volt_Min':
                #jsonArray[y]['{}'.format(temp[z])]=float(jsonArray[y]['{}'.format(temp[z])])  
            #elif str(temp[z])=='LVDT1':
                #jsonArray[y]['{}'.format(temp[z])]=float(jsonArray[y]['{}'.format(temp[z])])     
            elif str(temp[z])=='TIMESTAMP':
                temp1=jsonArray[y]['{}'.format(temp[z])]
                temp2=datetime.strptime(temp1, '"%Y-%m-%d %H:%M:%S"')
                temp3=datetime.strftime(temp2, '"%Y-%m-%dT%H:%M:%S.%fZ"')
                temp4=datetime.strptime(temp3, '"%Y-%m-%dT%H:%M:%S.%fZ"')
                jsonArray[y]['{}'.format(temp[z])]=temp4
                #print(type(temp4))
            elif str(temp[z])=='RECORD':
                temp1=str(jsonArray[y]['{}'.format(temp[z])])
                temp2=""
                for i in range(0,len(temp1)):
                    if temp1[i]!=" ":
                        temp2=temp2+temp1[i]
                    elif temp1[i]==" ":
                        break
                jsonArray[y]['RECORD']=str(temp2)
    st_name=[]
    temp=jsonArray[0]['RECORD']
    for y in range(0,len(jsonArray)):
        if temp==str(jsonArray[y]['RECORD']):
            temp=str(jsonArray[y]['RECORD'])
            if y==len(jsonArray)-1:
                st_name.append(str(jsonArray[y]['RECORD']))
        elif temp!=str(jsonArray[y]['RECORD']):
            st_name.append(str(jsonArray[y]['RECORD']))
            temp=str(jsonArray[y]['RECORD'])
    return jsonArray,st_name


def setdata(jsonArray):           
    json_body = []
    temp_dict=list(jsonArray[0])
    counter1=0
    for record in jsonArray:
        record1={}
        record2={}
        for i in range(0,len(temp_dict)):
            if type(record[temp_dict[i]])==str:
                record1['{}'.format(temp_dict[i])] = record[temp_dict[i]]
            elif type(record[temp_dict[i]])==datetime:
                temp1=datetime.strftime(record[temp_dict[i]], '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                record2['{}'.format(temp_dict[i])] = float(record[temp_dict[i]])
        if (record2['LVDT2']<-99998.0) and counter1==0:
            print(record1)
            counter1=counter1+1
        json_body.append(
            {
                #"measurement":"{}".format(record['RECORD']),
                "measurement":"土庫國中",
                "time":temp1,
                "tags":record1,
                "fields":record2
                    
            }
        )
    return json_body


def insertdatabase(json_body):
    bucket = ""
    url=""
    token=""
    org=""
    client = influxdb_client.InfluxDBClient(
        url=url,
        token=token,
        org=org
    )

    write_api = client.write_api(write_options=WriteOptions(batch_size=1000,
                                                          flush_interval=10000,
                                                          jitter_interval=2000,
                                                          retry_interval=5000,
                                                          max_retries=5,
                                                          max_retry_delay=30000,
                                                          exponential_base=2))
    write_api.write(bucket=bucket, org=org, record=json_body)
start=time.time()
file=searchfile() #search data_file in file
for i in range(0,len(file)):
    org_data , column = updata(file[i]) 
    json_array , st_name=changeformat(org_data,column)
    json_body = setdata(json_array)
    insertdatabase(json_body)
#print(json_array[0:3])
end = time.time()
end = end-start
end = round(end,4)
print(end)
