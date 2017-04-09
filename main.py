from Library import DataRequest
import sys
import csv

def main():
    req = DataRequest();
    # read locations csv data and get latitude and longitude data from google api
    locations_data=req.get_lat_long_data('locations.csv')

    api_key=sys.argv[1];
    # get data from forecast.io
    weather_data=req.store_weather_data(locations_data,api_key)

    # store in csv

    req.store_in_csv(weather_data)











if __name__=="__main__":
    main()
