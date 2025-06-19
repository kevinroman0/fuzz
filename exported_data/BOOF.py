

import pandas as pd
import requests
import json
import os
import time


from pymongo import MongoClient

# Connect to MongoDB (adjust the URI as needed)

uri = "mongodb+srv://romankevin176:d2ew7cN9U3VMV0Yj@fuzz.vkfskgo.mongodb.net/?retryWrites=true&w=majority&appName=fuzz"
# Create a new client and connect to the server

client = MongoClient(uri)  # default local MongoDB
db = client['weather_db']  # Database name
collection = db['history_weather']  # Collection name
collection.delete_many({})  # Clear the collection before inserting new data

csv = 'sorted_by_year_ca.csv'
output_file = 'out.csv'

df_input = pd.read_csv(csv)



# api limit has 

for i, row in df_input.iterrows():
    lat = row['incident_latitude']
    lon = row['incident_longitude']
    name = row['incident_name']
    start_date = row['incident_date_created'].split(' ')[0]
    end_date = row['incident_date_last_update'].split('T')[0]
    listy = ",".join([
    "temperature_2m",
    "relative_humidity_2m",
    "dew_point_2m",
    "apparent_temperature",
    "pressure_msl",
    "surface_pressure",
    "cloudcover",
    "cloudcover_low",
    "cloudcover_mid",
    "cloudcover_high",
    "windspeed_10m",
    "windgusts_10m",
    "winddirection_10m",
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "terrestrial_radiation",
    "precipitation",
    "rain",
    "snowfall",
    "weathercode",
    "et0_fao_evapotranspiration",
    "vapour_pressure_deficit",
    "soil_temperature_0cm",
    "soil_temperature_6cm",
    "soil_temperature_18cm",
    "soil_temperature_54cm",
    "soil_moisture_0_1cm",
    "soil_moisture_1_3cm",
    "soil_moisture_3_9cm",
    "soil_moisture_9_27cm",
    "soil_moisture_27_81cm"
])  
    
    
    # url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&start={start_date}&end={end_date}&type=hour&appid=3459df5824c519bea0f8fa026299b5ef' 
    url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly={listy}&timezone=America/Los_Angeles'
    response = requests.get(url)



    if response.status_code == 200:
        if i != 0 and i % 500 == 0:
            time.sleep(60) # Sleep for 60 seconds after every 600 requests to avoid hitting API limits
        
        data = response.json()
        data['incident_name'] = name    #adds the incident name
        # json_str = json.dumps(data)
        collection.insert_one(data)
        # df = pd.DataFrame([[json_str]], columns=["json_response"])
        
        # file_exists = os.path.exists(output_file)
        # df.to_csv(output_file, index=False, mode='a', header=not file_exists)
    else:
        print(f"Failed to fetch data for lat={lat}, lon={lon}. Status code: {response.status_code}")
        print(f'{response.text}')

        # main goal is to add the firename and make the overall identifier



