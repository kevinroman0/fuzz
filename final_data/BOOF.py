import pandas as pd
import requests
import time
import ee
from pymongo import MongoClient
from retrying import retry
from datetime import datetime

# Initialize Earth Engine
ee.Initialize(project='fuzz-464001')

# MongoDB setup
uri = "mongodb+srv://romankevin176:d2ew7cN9U3VMV0Yj@fuzz.vkfskgo.mongodb.net/?retryWrites=true&w=majority&appName=fuzz"
client = MongoClient(uri)
db = client['weather_db']
collection = db['history_weather']
collection.delete_many({})  # Clear existing data

# Load incident data
csv_path = '../dont_delete/sorted_by_year.csv'
df_input = pd.read_csv(csv_path)

# LANDFIRE datasets to fetch (updated to modern versions)
LANDFIRE_DATASETS = {
    'EVT': 'LANDFIRE/US_200/ExistingVegetationType',  # Vegetation type
    'EVH': 'LANDFIRE/US_200/ExistingVegetationHeight',  # Vegetation height (biomass proxy)
    'FBFM40': 'LANDFIRE/US_200/FBFM40',  # Fuel models
    'CC': 'LANDFIRE/US_200/ForestCanopyCover'  # Canopy cover (%)
}

def retry_if_ee_error(exception):
    """Retry on Earth Engine errors."""
    return isinstance(exception, ee.EEException)

@retry(retry_on_exception=retry_if_ee_error, stop_max_attempt_number=3, wait_fixed=2000)
def get_gee_data(lat, lon):
    """Fetch LANDFIRE and other fire-relevant data from GEE."""
    point = ee.Geometry.Point([lon, lat])
    results = {}
    
    for band, dataset in LANDFIRE_DATASETS.items():
        try:
            img = ee.Image(dataset)
            value = img.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=point,
                scale=30,
                maxPixels=1e9
            ).get(band).getInfo()
            results[band] = value
        except Exception as e:
            print(f"Error fetching {band}: {e}")
            results[band] = None
    
    return results if any(results.values()) else None

def fetch_weather_data(lat, lon, start_date, end_date):
    """Fetch historical weather data from Open-Meteo."""
    params = [
        "temperature_2m", "relative_humidity_2m", "dew_point_2m",
        "wind_speed_10m", "precipitation"  # Added wind and precipitation for fire risk
    ]
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}"
        f"&hourly={','.join(params)}&timezone=America/Los_Angeles"
    )
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Weather API failed: {e}")
        return None

# Main processing loop
for i, row in df_input.iterrows():
    try:
        lat = row['incident_latitude']
        lon = row['incident_longitude']
        name = row['incident_name']
        start_date = str(row['incident_date_created']).split(' ')[0]
        end_date = str(row['incident_date_extinguished']).split('T')[0]
        burned = row['incident_acres_burned']

        print(f"\nProcessing {i+1}/{len(df_input)}: {name} at ({lat}, {lon})")

        # Fetch weather data
        weather_data = fetch_weather_data(lat, lon, start_date, end_date)
        if not weather_data:
            continue

        # Fetch GEE (LANDFIRE) data
        gee_data = get_gee_data(lat, lon)
        if not gee_data:
            print(f"Skipping {name} - no LANDFIRE data")
            continue

        # Compile document for MongoDB
        doc = {
            "incident_name": name,
            "location": {"lat": lat, "lon": lon},
            "dates": {"start": start_date, "end": end_date},
            "acres_burned": burned,
            "weather": weather_data,
            "landfire": gee_data,
            "processed_at": datetime.utcnow().isoformat()
        }

        # Insert into MongoDB
        collection.insert_one(doc)
        print(f"✅ Saved {name}")

        # Rate limiting (1 request every 2 seconds)
        time.sleep(2) if i % 10 == 0 else None

    except Exception as e:
        print(f"🚨 Error on row {i}: {e}")
        continue

print("\nProcessing complete! Data saved to MongoDB.")