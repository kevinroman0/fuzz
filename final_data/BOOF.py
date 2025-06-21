

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
csv = '../dont_delete/sorted_by_year.csv'


output_file = 'out.csv'

df_input = pd.read_csv(csv)



for i, row in df_input.iterrows():
    lat = row['incident_latitude']
    lon = row['incident_longitude']
    name = row['incident_name']

    start_date = row['incident_date_created'].split(' ')[0]
    end_date = str(str(row['incident_date_extinguished']).split('T')[0])
    print(start_date)
    print(end_date)
    burned = row['incident_acres_burned']
    

    # 🌤 WEATHER API
    listy = ",".join([
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "apparent_temperature", "pressure_msl", "surface_pressure",
        "cloudcover", "cloudcover_low", "cloudcover_mid", "cloudcover_high",
        "windspeed_10m", "windgusts_10m", "winddirection_10m",
        "shortwave_radiation", "direct_radiation", "diffuse_radiation",
        "direct_normal_irradiance", "terrestrial_radiation",
        "precipitation", "rain", "snowfall", "weathercode",
        "et0_fao_evapotranspiration", "vapour_pressure_deficit",
        "soil_temperature_0cm", "soil_temperature_6cm",
        "soil_temperature_18cm", "soil_temperature_54cm",
        "soil_moisture_0_1cm", "soil_moisture_1_3cm",
        "soil_moisture_3_9cm", "soil_moisture_9_27cm",
        "soil_moisture_27_81cm"
    ])
    weather_url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly={listy}&timezone=America/Los_Angeles'
    weather_resp = requests.get(weather_url)

    # 🌲 LANDFIRE API
    landfire_url = "https://landfire.cr.usgs.gov/arcgis/rest/services/Landfire/LF_2020/MapServer/identify"
    params = {
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "sr": 4326,
        "layers": "all",  # or specify layer IDs (e.g., "5" for fuel models)
        "tolerance": 2,
        "f": "json"
    }

    landfire_resp = requests.get(landfire_url, params=params)

    # ✅ Insert into MongoDB if both responses are good
    if weather_resp.status_code == 200 and landfire_resp.status_code == 200:
        weather_data = weather_resp.json()
        landfire_data = landfire_resp.json()

        combined_doc = {
            "incident_name": name,
            "location": {"lat": lat, "lon": lon},
            "incident_acres_burned": burned,
            "weather_data": weather_data,
            "fuel_data": landfire_data
        }

        collection.insert_one(combined_doc)
        print(f"Inserted weather + fuel data for {name} at {lat},{lon}")

        if i != 0 and i % 500 == 0:
            time.sleep(60)  # Avoid API rate limits

    else:
        if weather_resp.status_code != 200:
            print(f"Weather API failed for {name}: {weather_resp.status_code}")
            print(weather_resp.text)

        if landfire_resp.status_code != 200:
            print(f"LANDFIRE API failed for {name}: {landfire_resp.status_code}")
            print(landfire_resp.text)
