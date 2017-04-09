from Library import DataRequest
import sys
import csv

def main():
    req = DataRequest();
    # read locations csv data and get latitude and longitude data from google api
    locations_data=req.get_lat_long_data('locations.csv')

    # get data from forecast.io
    wetaher_data=req.store_weather_data(locations_data)

    # store in csv
    req.store_in_csv(wetaher_data)











if __name__=="__main__":
    main()
