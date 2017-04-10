import requests
import sys
import datetime
import csv
import time
import json


class DataRequest:

    def get_request(self,url,payload):
        try:

            response = requests.get(url,params=payload)



        except requests.exceptions.RequestException as e:
            print e
            sys.exit(1)

        # print  response.status_code
        if (response.status_code == 200):
            try:
                json= response.json()
            except ValueError as e:
                print e
                sys.exit(1)
        else:
            print response.status_code
            sys.exit(1)

        return json


    def get_lat_long_data(self,path):
        location_lst=[]
        with open(path,'rb') as f:
            reader = csv.DictReader(f)

            for row in reader:
                time.sleep(0.2) #rate limit delay
                payload={'address':row['postal_code']}
                url='http://maps.googleapis.com/maps/api/geocode/json?'
                geoData=self.get_request(url,payload)
                if geoData['status']=="ZERO_RESULTS":
                    print geoData
                    print "Invalid PostalCode: "+row['postal_code']
                else:
                    lat_long_data=geoData['results'][0]['geometry']['location']
                    row['latitude']=lat_long_data['lat']
                    row['longitude']=lat_long_data['lng']
                    row['formatted_addesss']=geoData['results'][0]['formatted_address']
                    row['long_name']=geoData['results'][0]['address_components'][1]['long_name']
                    location_lst.append(row)

        return location_lst




    def store_weather_data(self,locations_data,api_key):
        daily_data_lst=[]
        for location in locations_data:
            # print location
            datetime.datetime.now().isoformat()
            start_date=datetime.datetime.fromtimestamp(int(location['date_first']))
            end_date=datetime.datetime.fromtimestamp(int(location['date_last']))
            delta = datetime.timedelta(days=1)
            d=start_date
            while d <= end_date:
                timestamp= d.strftime("%Y-%m-%dT%H:%M:%S")
                payload={}
                url='https://api.darksky.net/forecast/'+api_key+'/'+str(location['latitude'])+","+str(location['longitude'])+","+timestamp
                weatherData=self.get_request(url,payload)
                # print weatherData
                daily_data=weatherData['daily']['data'][0]
                daily_data['formatted_ date']=d.strftime("%Y-%m-%d")
                daily_data['formatted_address']=location['formatted_addesss']
                daily_data['long_name']=location['long_name']
                daily_data['latitude']=location['latitude']
                daily_data['longitude']=location['longitude']
                daily_data_lst.append(daily_data)
                d += delta
        return daily_data_lst



    def store_in_csv(selfself,data_list):
        header=['formatted_address','long_name','latitude','longitude','formatted_ date','time','summary','icon','sunriseTime','sunsetTime','moonPhase','precipIntensity','precipIntensityMax','precipIntensityMaxTime','precipProbability','precipType','precipAccumulation',
                'temperatureMin','temperatureMinTime','temperatureMax','temperatureMaxTime','apparentTemperatureMin','apparentTemperatureMinTime','apparentTemperatureMax','apparentTemperatureMaxTime',
                'dewPoint','humidity','windSpeed','windBearing','visibility','cloudCover','pressure']

        weather = open('Data.csv', 'wb')
        csvwriter = csv.writer(weather)
        csvwriter.writerow(header)
        for data in data_list:
            # if row is blank i.e we do not have any response, then we do not insert anything into the csv
             if len(data)==0: continue
            # csvrow is an array that stores the content that will be inserted into the CSV
             csvrow=[]
            # for each header value we query the response and store the values in csv
             for j in range(len(header)):
                 # for each header value if we do not have any response then store blank values in CSV
                 if(header[j] in data.keys()): csvrow.append(data[header[j]])
                 else: csvrow.append("")
             csvwriter.writerow(csvrow)

        weather.close()
        print("Data Transfered and saved in /src/Data.csv")
