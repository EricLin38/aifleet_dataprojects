#script reads API for given date range to return formatted CSV file containing
#driver end times adjusted for respective local time zones with Daylight Savings
#time accounted for

import json
import pandas as pd
import requests
from datetime import datetime
from datetime import timedelta
from geopy.geocoders import Nominatim
from timezonefinder import TimezoneFinder
from us import states

#set static variables needed throughout script
daylightstart2022= datetime(2022,3,13)
daylightend2022= datetime(2022,11,6)
geolocator = Nominatim(user_agent="geoapiExercises")
obj = TimezoneFinder()

#setting timezone conversions based on locations stored locally for faster processing
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

#requests data from the api using a specific date, returns a json file with the data
def new_json(date):
    headers = {
        #removed for data security
        }
    file_date=datetime.strftime(date,'%Y-%m-%d')
    response = requests.get("https://api.keeptruckin.com/v1/logs?start_date="+file_date+"&end_date="+file_date+"&per_page=100", headers=headers)
    data=response.text
    json_temp=json.loads(data)
    
    return json_temp

#checks if an inputted date is daylight savings or not
def is_daylight(day):
    return True if day>daylightstart2022 and day<daylightend2022 else False

#iterates through the data to find the final event entry for
#each driver and maps the location of that event to a time zone
#it also converts each entry to local time for the driver
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

#enters the specific dates to search, can filter down certain days of the week
#returns as a csv
def main():
    startdate = input('Enter Start Date (YYYY-MM-DD): ') 
    enddate = input('Enter End Date (YYYY-MM-DD): ')
    date_list = pd.date_range(start=startdate,end=enddate)
    friday_list=[i for i in date_list if i.day_name() == 'Friday']
    primarydf=pd.DataFrame()
    for day in date_list:
        primarydf=pd.concat([primarydf,findend(new_json(day),day)], axis=0)
    primarydf.to_csv('endtimes'+startdate+'_'+enddate+'.csv')
    return

main()
