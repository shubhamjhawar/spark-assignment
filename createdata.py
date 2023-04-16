import requests 
import pandas as pd
import json
import csv
from apikeys import data,hostname
def get_data_and_convert_to_csv():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
            "X-RapidAPI-Key": data,
            "X-RapidAPI-Host": hostname,
    }

    response = requests.request("GET", url, headers=headers)
    print(type(response.json()))
    json_data = response.json() 
    json_data.popitem()
    json_data.popitem()
    field_names = ['slno', 'state', 'confirm','cured','death','active','total']
    with open('coviddata1.csv', mode='w', newline='') as file:

        # create a CSV writer
        writer = csv.writer(file)
        
        # write the header row
        writer.writerow(['slno', 'state', 'confirm','cured','death','total'])
        
        # write the data rows
        for state_data in json_data.values():
            writer.writerow([state_data['slno'], state_data['state'], state_data['confirm'],state_data['cured'],state_data['death'],state_data['total']])


if __name__ == '__main__':
        get_data_and_convert_to_csv()


