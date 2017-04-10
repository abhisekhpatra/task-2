import requests
import sys
import datetime
import csv
import time



# Data Request Library provides interface to query information from forecast api and google geocode api

class DataRequest:

    # get_request is an interface for sending http requests to api's and returns the response in JSON. It takes two parameters: 1. url and 2.payload which is data that is sent in the url"s query string
    def get_request(self,url,payload):
        # try block for network exceptions
        try:

            response = requests.get(url,params=payload)



        except requests.exceptions.RequestException as e:
            print e
            sys.exit(1)


        if (response.status_code == 200):
            # try catch block for ValueError Exception in response
            try:
                # submit  response json extract
                json= response.json()
            except ValueError as e:
                print e
                sys.exit(1)
        else:
            print response.status_code
            sys.exit(1)

        return json

    # get_lat_long_data is an interface for extracting laitude and longitude information from google. It takes the filepath as a parameter where the location postal codes are provided and returns a list of location details(the lat and long values)
    def get_lat_long_data(self,path):
        location_lst=[]
        with open(path,'rb') as f:
            reader = csv.DictReader(f)
            # loop over each location row in the csv
            for row in reader:
                time.sleep(0.2) #rate limit delay
                # pauload is the url query argument
                payload={'address':row['postal_code']}
                url='http://maps.googleapis.com/maps/api/geocode/json?'
                # send request to googleapi and store data in geoData
                geoData=self.get_request(url,payload)
                # check geoData for misssing values incase of invalide postal codes
                if geoData['status']=="ZERO_RESULTS":
                    print geoData
                    print "Invalid PostalCode: "+row['postal_code']
                else:
                    # query the location from the result.
                    lat_long_data=geoData['results'][0]['geometry']['location']
                    # store lat and long values for later use
                    row['latitude']=lat_long_data['lat']
                    row['longitude']=lat_long_data['lng']
                    #store location address for user friendly analysis later
                    row['formatted_addesss']=geoData['results'][0]['formatted_address']
                    row['long_name']=geoData['results'][0]['address_components'][1]['long_name']
                    location_lst.append(row)

        return location_lst



    # store_weather _data is an interface for extracting weather information from forecast.io. It takes a list of location and the forecast api key as a parameter and returns a list of weather data for a given time range
    def store_weather_data(self,locations_data,api_key):
        daily_data_lst=[]
        for location in locations_data:
            datetime.datetime.now().isoformat()
            # for each location get date range
            start_date=datetime.datetime.fromtimestamp(int(location['date_first']))
            end_date=datetime.datetime.fromtimestamp(int(location['date_last']))
            # add delta of 1 day to loop through dates in between date ranges
            delta = datetime.timedelta(days=1)
            d=start_date
            while d <= end_date:
                # convert timestamp to format accepted by forecast.io

                timestamp= d.strftime("%Y-%m-%dT%H:%M:%S")
                payload={}
                url='https://api.darksky.net/forecast/'+api_key+'/'+str(location['latitude'])+","+str(location['longitude'])+","+timestamp
                weatherData=self.get_request(url,payload)
                # query forecast.io data for extracting weather related information
                daily_data=weatherData['daily']['data'][0]
                # along with weather data storing additional information for later analysis
                daily_data['formatted_date']=d.strftime("%Y-%m-%d")
                daily_data['formatted_address']=location['formatted_addesss']
                daily_data['long_name']=location['long_name']
                daily_data['latitude']=location['latitude']
                daily_data['longitude']=location['longitude']
                daily_data_lst.append(daily_data)
                d += delta
        return daily_data_lst


    # store_in_csv is an interface for storing data into CSV. It takes a list and stores it into Data.csv
    def store_in_csv(selfself,data_list):
    # create a list of headers we want in our csv. I had to do this manually as forecast.io does not provide an exhaustive list of all the variables in its response. Some variables are optional
        header=['formatted_address','long_name','latitude','longitude','formatted_date','time','summary','icon','sunriseTime','sunsetTime','moonPhase','precipIntensity','precipIntensityMax','precipIntensityMaxTime','precipProbability','precipType','precipAccumulation',
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
                 #***This code can be modified to show a warning when forecast.io adds a new variable  and update their response. We may need to add it in the header array.
                 if(header[j] in data.keys()): csvrow.append(data[header[j]])
                 else: csvrow.append("")
             csvwriter.writerow(csvrow)

        weather.close()
        print("Data Transfered and saved in Data.csv")
