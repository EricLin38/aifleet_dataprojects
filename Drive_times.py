import json
import pandas as pd
import requests
from datetime import timedelta, datetime
import timeit

def new_json(use_date):
    headers = {
            'x-api-key': '0e6160bb-d3e0-4a76-b474-250c13f6eed0',
            'User-Agent': 'Intern_Script',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
            }
    file_date=datetime.strftime(use_date,'%Y-%m-%d')
    response = requests.get("https://api.keeptruckin.com/v1/logs?start_date="+file_date+"&end_date="+file_date+"&per_page=100", headers=headers)
    data=response.text
    file=json.loads(data)
    return file

def drivetime(data, date):
    file_date=datetime.strftime(date,'%Y-%m-%d')
    dates=[file_date]
    dic={x["log"]["driver_company_id"]:str(timedelta(seconds=x["log"]["driving_duration"])) for x in data["logs"] if x["log"]["driving_duration"] >0}
    new_df=pd.DataFrame.from_dict(dic, orient='index')
    x=new_df.transpose()
    x.index=[dates]
    return x

def main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')    
    date_list = pd.date_range(start=startdate,end=enddate)
    dow_list=[i for i in date_list if i.day_name() == 'Saturday'
              or i.day_name()=='Friday']
    primarydf=pd.DataFrame()
    for day in date_list:
        primarydf=pd.concat([primarydf,drivetime(new_json(day),day)], axis=0)
    primarydf.to_csv('drivetimes'+startdate+'_'+enddate+'.csv')
    return 

start = timeit.default_timer()

main()

stop = timeit.default_timer()

print('Time: ', stop - start) 

