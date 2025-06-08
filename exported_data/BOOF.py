import pandas as pd
import requests
import json
import os


from pymongo import MongoClient

# Connect to MongoDB (adjust the URI as needed)

uri = "mongodb+srv://romankevin176:d2ew7cN9U3VMV0Yj@fuzz.vkfskgo.mongodb.net/?retryWrites=true&w=majority&appName=fuzz"
# Create a new client and connect to the server

client = MongoClient(uri)  # default local MongoDB
db = client['weather_db']  # Database name
collection = db['history_weather']  # Collection name

csv = 'converted_timestamps.csv'
output_file = 'out.csv'

df_input = pd.read_csv(csv)

for _, row in df_input.iterrows():
    lat = row['incident_latitude']
    lon = row['incident_longitude']
    start_date = row['incident_date_created']
    end_date = row['incident_date_last_update']
    
    url = f'https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&start={start_date}&end={end_date}&type=hour&appid=3459df5824c519bea0f8fa026299b5ef' 
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        # json_str = json.dumps(data)
        collection.insert_one(data)
        
        # df = pd.DataFrame([[json_str]], columns=["json_response"])
        
        # file_exists = os.path.exists(output_file)
        # df.to_csv(output_file, index=False, mode='a', header=not file_exists)
    else:
        print(f"Failed to fetch data for lat={lat}, lon={lon}. Status code: {response.status_code}")
