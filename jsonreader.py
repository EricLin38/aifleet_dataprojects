#combines all the functions of converting json files to read start, end, and drive times

#module containing the libraries and static variables needed for calculating start, end, and drive times

#run these lines in command line/terminal if the packages are not yet installed on your local machine
#pip install json
#pip install pandas
#pip install requests
#pip install datetime
#pip install geopy
#pip install timezonefinder
#pip install us

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

#run API call to retrieve json file
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

#calculate start times
def findstart(data,date):
    temp={}
    coordinates={}
    file_date=datetime.strftime(date,'%Y-%m-%d')
    dates=[file_date]
    if is_daylight(date):
        dl=timezones_daylight
    else:
        dl=timezones_standard
    for x in data["logs"]:
        for y in x["log"]["events"]:
            if y["event"]["type"] == 'driving' and x["log"]["driver_company_id"] not in temp.keys():
                try:                  
                    s = y["event"]["location"]
                    #pulls last 2 letters from the location string (which is the state abbreviation)
                    #and searches for time zone using state info. If state falls into the 'non exception'
                    #group, geolocation pinpoints precise timezone since these states contain 2+ timezones
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
                    coordinates[x["log"]["driver_company_id"]]=tz
                    temp[x["log"]["driver_company_id"]] = y["event"]["start_time"][11:19]
                except:
                    pass
    finaldict={i:datetime.strftime(datetime.strptime(temp[i],"%H:%M:%S")-coordinates[i],"%H:%M:%S") for i in temp if i in coordinates.keys()}
    new_df=pd.DataFrame.from_dict(finaldict, orient='index')
    x=new_df.transpose()
    x.index=[dates]
    return x

#calculate end times
def findend(data,date):
    temp={}
    coordinates={}
    file_date=datetime.strftime(date,'%Y-%m-%d')
    dates=[file_date]
    if is_daylight(date):
        dl=timezones_daylight
    else:
        dl=timezones_standard
    for x in data["logs"]:
        if len(x["log"]["events"]) > 1:
            try:                  
                s = x["log"]["events"][-1]["event"]['location']
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
                coordinates[x["log"]["driver_company_id"]]=tz
                temp[x["log"]["driver_company_id"]] = x["log"]["events"][-1]["event"]['start_time'][11:19]
            except:
                pass
    finaldict={i:datetime.strftime(datetime.strptime(temp[i],"%H:%M:%S")-coordinates[i],"%H:%M:%S") for i in temp if i in coordinates.keys()}
    new_df=pd.DataFrame.from_dict(finaldict, orient='index')
    x=new_df.transpose()
    x.index=[dates]
    return x

#calculate drive times
def finddrive(data, date):
    file_date=datetime.strftime(date,'%Y-%m-%d')
    dates=[file_date]
    dic={x["log"]["driver_company_id"]:str(timedelta(seconds=x["log"]["driving_duration"])) for x in data["logs"] if x["log"]["driving_duration"] > 0}
    new_df=pd.DataFrame.from_dict(dic, orient='index')
    x=new_df.transpose()
    x.index=[dates]
    return x

'''
#main function to write drive times dataframe
def dt_main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')    
    date_list = pd.date_range(start=startdate,end=enddate)
    primarydf=pd.DataFrame()
    for day in date_list:
        primarydf=pd.concat([primarydf,finddrive(new_json(day),day)], axis=0)
    primarydf.to_csv('drivetimes'+startdate+'_'+enddate+'.csv')
    return 

#main function to write start times dataframe
def st_main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')
        
    date_list = pd.date_range(start=startdate,end=enddate)
    primarydf=pd.DataFrame()
    for day in date_list:
        primarydf=pd.concat([primarydf,findstart(new_json(day),day)], axis=0)
    primarydf.to_csv('start_times'+startdate+'_'+enddate+'.csv')
    return 

#main function to write end times dataframe
def et_main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')
        
    date_list = pd.date_range(start=startdate,end=enddate)
    primarydf=pd.DataFrame()
    for day in date_list:
        primarydf=pd.concat([primarydf,findend(new_json(day),day)], axis=0)
    primarydf.to_csv('endtimes'+startdate+'_'+enddate+'.csv')
    return 
'''

def main():
    question = input("Calculate Start times, End times, or Drive times? (Type s/e/d)")
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ') 
    date_list = pd.date_range(start=startdate,end=enddate)
    primarydf=pd.DataFrame()
    
    if question == 's':
        for day in date_list:
            primarydf=pd.concat([primarydf,findstart(new_json(day),day)], axis=0)
        primarydf.to_csv('start_times'+startdate+'_'+enddate+'.csv')
    elif question == 'e':
        for day in date_list:
            primarydf=pd.concat([primarydf,findend(new_json(day),day)], axis=0)
        primarydf.to_csv('endtimes'+startdate+'_'+enddate+'.csv')
    elif question == 'd':
        for day in date_list:
            primarydf=pd.concat([primarydf,finddrive(new_json(day),day)], axis=0)
        primarydf.to_csv('drivetimes'+startdate+'_'+enddate+'.csv')
    else:
        print('Input Invalid, Program Ending')
    return 

start = timeit.default_timer()

main()

stop = timeit.default_timer()

print('Time: ', stop - start) 
