#!/usr/bin/env python
# coding: utf-8

# In[1]:


#module containing the libraries and static variables needed for calculating start, end, and drive times
#pip install us
#pip install json
#pip install pandas
#pip install requests
#pip install datetime
#pip install geopy
#pip install timezonefinder

import json 
import pandas as pd 
import requests 
from datetime import timedelta, datetime 
from geopy.geocoders import Nominatim 
from timezonefinder import TimezoneFinder 
from us import states 
import timeit

daylightstart2022= datetime(2022,3,13)
daylightend2022= datetime(2022,11,6)
geolocator = Nominatim(user_agent="geoapiExercises")
obj = TimezoneFinder()

timezones_daylight={
    "America/Chicago": timedelta(hours=5),
    "America/New_York": timedelta(hours=4),
    "America/Phoenix": timedelta(hours=7),
    "America/Los_Angeles": timedelta(hours=7),
    "America/Denver": timedelta(hours=6),
    "America/Boise": timedelta(hours=6),
    "America/Detroit": timedelta(hours=4),
    "America/Indiana/Knox": timedelta(hours=5),
    "America/Indiana/Marengo": timedelta(hours=4),
    "America/Indiana/Petersburg": timedelta(hours=4),
    "America/Indiana/Tell_City": timedelta(hours=5),
    "America/Indiana/Vevay": timedelta(hours=4),
    "America/Indiana/Vincennes": timedelta(hours=4),
    "America/Indiana/Winamac": timedelta(hours=4),
    "America/Kentucky/Louisville": timedelta(hours=4),
    "America/Kentucky/Monticello": timedelta(hours=5),
    "America/North_Dakota/Beulah": timedelta(hours=5),
    "America/North_Dakota/Center": timedelta(hours=5),
    "America/North_Dakota/New_Salem": timedelta(hours=6)
    }

timezones_standard={
    "America/Chicago": timedelta(hours=6),
    "America/New_York": timedelta(hours=5),
    "America/Phoenix": timedelta(hours=7),
    "America/Los_Angeles": timedelta(hours=8),
    "America/Denver": timedelta(hours=7),
    "America/Boise": timedelta(hours=7),
    "America/Detroit": timedelta(hours=5),
    "America/Indiana/Knox": timedelta(hours=6),
    "America/Indiana/Marengo": timedelta(hours=5),
    "America/Indiana/Petersburg": timedelta(hours=5),
    "America/Indiana/Tell_City": timedelta(hours=6),
    "America/Indiana/Vevay": timedelta(hours=5),
    "America/Indiana/Vincennes": timedelta(hours=5),
    "America/Indiana/Winamac": timedelta(hours=5),
    "America/Kentucky/Louisville": timedelta(hours=5),
    "America/Kentucky/Monticello": timedelta(hours=6),
    "America/North_Dakota/Beulah": timedelta(hours=6),
    "America/North_Dakota/Center": timedelta(hours=6),
    "America/North_Dakota/New_Salem": timedelta(hours=7)
    }

exceptions={
    "TX": "America/Chicago",
    "OR": "America/Los_Angeles",
    "ND": "America/North_Dakota/Center",
    "NV": "America/Los_Angeles",
    "KS": "America/Chicago",
    "ID": "America/Denver"
    }

not_exceptions={
    'TN', 'SD', 'NE', 'MI', 'KY', 'IN', 'FL'}

#determine if inputed date is during or outside of daylight savings
def is_daylight(day):
    return True if day>daylightstart2022 and day<daylightend2022 else False

#handles time zone processing and accounts for DST using is_daylight function
#input any location (formatted as city, ST with the option to include 'x mi north of city, ST')
def zoner(s,date):
    #calculates the timezone and sets a dictionary to use based on if DST is active
    if is_daylight(date):
        dl=timezones_daylight
    else:
        dl=timezones_standard
    
    #takes inputted location string and parses either a state or geolocates the city if the state has multiple time zones
    if s[-2::] not in exceptions.keys() and s[-2::] not in not_exceptions:
        tz=dl[states.lookup(s[-2::]).time_zones[0]]
    elif s[-2::] in exceptions.keys() and s[-2::] not in not_exceptions:
        tz = dl[exceptions[s[-2::]]]
    else:
        before_keyword, keyword, after_keyword = s.partition('of ')
        if after_keyword:  
            location = geolocator.geocode(str(after_keyword))
        else:
            location = geolocator.geocode(str(s))
        tz=dl[obj.timezone_at(lng=location.longitude, lat=location.latitude)]
    return tz


#takes dictionary and converts to dataframe using date passed in as the index; the dataframe only has that one row as 
def to_df(date, dic):
    file_date=datetime.strftime(date,'%Y-%m-%d')
    dates=[file_date]
    new_df=pd.DataFrame.from_dict(dic, orient='index')
    x=new_df.transpose()
    x.index=[dates]
    return x

#run API call to retrieve json file
#can pull logs, driver performance events, and scorecard summaries
#logs contain the best-formatted data needed to process daily events
def pull_logs(use_date):
    headers = {
            'x-api-key': '0e6160bb-d3e0-4a76-b474-250c13f6eed0',
            'User-Agent': 'DataOpsTeam',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
            }
    file_date=datetime.strftime(use_date,'%Y-%m-%d')
    #pulls only 100 entries, script will need updating once we grow beyond 100 drivers
    url="https://api.keeptruckin.com/v1/logs?start_date="+file_date+"&end_date="+file_date+"&per_page=100"
    response = requests.get(url, headers=headers)
    return json.loads(response.text)
def driver_perf_events(use_date):
    file_date=datetime.strftime(use_date,'%Y-%m-%d')
    url = "https://api.keeptruckin.com/v1/driver_performance_events?start_date="+file_date+"&end_date="+file_date+"&per_page=100&page_no=1"
    headers = {
        "Accept": "application/json",
        'Content-Type': 'application/json',
        "X-Api-Key": "0e6160bb-d3e0-4a76-b474-250c13f6eed0"
    }
    response = requests.get(url, headers=headers)
    return json.loads(response.text)
def scorecard_summary(use_date):
    file_date=datetime.strftime(use_date,'%Y-%m-%d')
    url = "https://api.keeptruckin.com/v1/scorecard_summary?start_date="+file_date+"&end_date="+file_date+"&per_page=100&page_no=1"
    headers = {
        "Accept": "application/json",
        'Content-Type': 'application/json',
        "X-Api-Key": "0e6160bb-d3e0-4a76-b474-250c13f6eed0"
    }
    response = requests.get(url, headers=headers)
    return json.loads(response.text)

#calculate start times
def findstart(data,date):
    temp={}
    coordinates={}
    for x in data["logs"]:
        for y in x["log"]["events"]:
            if y["event"]["type"] == 'driving' and x["log"]["driver_company_id"] not in temp.keys():
                try:                  
                    s = y["event"]["location"]
                    coordinates[x["log"]["driver_company_id"]]=zoner(s,date)
                    temp[x["log"]["driver_company_id"]] = y["event"]["start_time"][11:19]
                except:
                    pass
    return to_df(date, {i:datetime.strftime(datetime.strptime(temp[i],"%H:%M:%S")-coordinates[i],"%H:%M:%S") for i in temp if i in coordinates.keys()})

#calculate end times
def findend(data,date):
    temp={}
    coordinates={}
    for x in data["logs"]:
        if len(x["log"]["events"]) > 1:
            try:                  
                s = x["log"]["events"][-1]["event"]['location']
                coordinates[x["log"]["driver_company_id"]]=zoner(s,date)
                temp[x["log"]["driver_company_id"]] = x["log"]["events"][-1]["event"]['start_time'][11:19]
            except:
                pass
    return to_df(date,{i:datetime.strftime(datetime.strptime(temp[i],"%H:%M:%S")-coordinates[i],"%H:%M:%S") for i in temp if i in coordinates.keys()})

#calculate drive times
def finddrive(data, date):
    return to_df(date, {x["log"]["driver_company_id"]:str(timedelta(seconds=x["log"]["driving_duration"])) for x in data["logs"] if x["log"]["driving_duration"] > 0})

#main function to write dataframe for a given task
def main():
    question = input("Calculate Start times, End times, or Drive times? (Type s/e/d)")
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')    
    date_list = pd.date_range(start=startdate,end=enddate)
    primarydf=pd.DataFrame()
    if question == 's':
        for day in date_list:
            primarydf=pd.concat([primarydf,findstart(pull_logs(day),day)], axis=0)
    elif question == 'e':
        for day in date_list:
            primarydf=pd.concat([primarydf,findend(pull_logs(day),day)], axis=0)
    elif question == 'd':
        for day in date_list:
            primarydf=pd.concat([primarydf,finddrive(pull_logs(day),day)], axis=0)
    else:
        print('Input Invalid, Program Ending')
        return
    return primarydf


# In[2]:


#script to calculate home times for a given period by finding monday start times and subtracting the 
#corresponding Fri/Sat/Sun end time for each driver to find how long they spent at home for a given weekend
def main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')
    date_list = pd.date_range(start=startdate,end=enddate)
    monday_start=[i for i in date_list if i.day_name() == 'Monday']
    weekend=[i for i in date_list if i.day_name() == 'Friday' or i.day_name() == 'Saturday' or i.day_name() == "Sunday"]
    primarydf=pd.DataFrame()
    for day in date_list:
        if day in monday_start:
            primarydf=pd.concat([primarydf,findstart(new_json(day),day)], axis=0)
        elif day in weekend:
            primarydf=pd.concat([primarydf,findend(new_json(day),day)], axis=0)
    primarydf.to_csv('mon,weekend data ytd.csv')
    return

start = timeit.default_timer()

main()

stop = timeit.default_timer()

print('Time: ', stop - start) 


# In[3]:


#calculating sleeper berth periods
def sleepers(data,date):
    temp, coordinates, last_start, last_coord={}

    for x in data["logs"]:
        for y in x["log"]["events"]:
            if y["event"]["type"] == 'sleeper' and x["log"]["driver_company_id"] not in temp.keys():
                try:                  
                    s = y["event"]["location"]
                    coordinates[x["log"]["driver_company_id"]]=zoner(s,date)
                    temp[x["log"]["driver_company_id"]] = y["event"]["end_time"][11:19]
                except:
                    pass

            elif y["event"]["type"] == 'sleeper':
                try:                  
                    s = y["event"]["location"]
                    last_coord[x["log"]["driver_company_id"]]=zoner(s,date)
                    last_start[x["log"]["driver_company_id"]] = y["event"]["start_time"][11:19]
                except:
                    pass
    
    #when each driver ends their sleeper period in the morning
    first_final={i:datetime.strftime(datetime.strptime(temp[i],"%H:%M:%S")-coordinates[i],"%H:%M:%S") for i in temp if i in coordinates.keys()}
    
    #when each driver begins their sleeper period for the evening
    last_final={i:datetime.strftime(datetime.strptime(last_start[i],"%H:%M:%S")-last_coord[i],"%H:%M:%S") for i in last_start if i in last_coord.keys()}
    
    return to_df(date, first_final), to_df(date, last_final)

def main():
    startdate = '2022-04-01'
    enddate = '2022-04-05'  
    date_list = pd.date_range(start=startdate,end=enddate)
    awake=pd.DataFrame()
    bedtime=pd.DataFrame()
    for day in date_list:
        start, end =sleepers(new_json(day),day)
        awake = pd.concat([awake,start], axis=0)
        bedtime = pd.concat([bedtime,end], axis=0)
    with pd.ExcelWriter('sleeper_output.xlsx') as writer:  
        awake.to_excel(writer, sheet_name='awake')
        bedtime.to_excel(writer, sheet_name='bedtime')
    return


# In[ ]:


startdate='2022-05-07'
enddate='2022-05-07'
date_list=pd.date_range(start=startdate,end=enddate)
[print(json.dumps(driver_perf_events(day), indent=4, sort_keys=True)) for day in date_list]
#print(json.dumps(scorecard_summary(startdate), indent=4, sort_keys=True))


# In[ ]:


#file=json.load(open('response0302.json'))
#file=json.load(open('response0301.json'))
use_date=datetime(2022,5,2)
print(json.dumps(pull_logs(use_date), indent=4, sort_keys=True))
#l = new_json(use_date)
#for y in l["logs"]:
#    print(len(y["log"]["events"]))


# In[4]:


use_date=datetime(2022,5,2)
x=pull_logs(use_date)
print(findend(x, use_date))


# In[6]:


for log in x['logs']:
    for y in log['log']['events']:
        print(y['event']['location'])


# In[10]:


x, y,z=1, 2, 3


# In[11]:


print(x)
print(y)
print(z)


# In[ ]:




