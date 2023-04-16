import requests 
import pandas as pd
import json
import csv

def get_data_and_convert_to_csv():
    url = "https://covid-19-india2.p.rapidapi.com/details.php"

    headers = {
            "X-RapidAPI-Key": "cd6bdbf7d4mshe14c31a94790f19p1ef9fdjsn5aa9c7d4c16f",
            "X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers)
    print(type(response.json()))
    json_data = response.json() 
    json_data.popitem()
    json_data.popitem()
    field_names = ['slno', 'state', 'confirm','cured','death','active','total']
    with open('coviddata.csv', mode='w', newline='') as file:

        # create a CSV writer
        writer = csv.writer(file)
        
        # write the header row
        writer.writerow(['slno', 'state', 'confirm','cured','death','total'])
        
        # write the data rows
        for state_data in json_data.values():
            writer.writerow([state_data['slno'], state_data['state'], state_data['confirm'],state_data['cured'],state_data['death'],state_data['total']])


if __name__ == '__main__':
        get_data_and_convert_to_csv()


