import requests
import json
import csv  
import urllib.request
import pandas as pd
#california natural resource agency 
url = 'https://data.cnra.ca.gov/api/3/action/datastore_search?resource_id=8ea91e09-3c84-4eba-9489-3ab3b08adfd0&limit=10000' 
# Send a GET request to the API
response = requests.get(url)
# Parse the JSON response
response_dict = response.json()
filtered_records = []
# Accessing records and printing the value of YEAR_ for each record
if "result" in response_dict and "records" in response_dict["result"]:
    for record in  response_dict["result"]["records"]:
        year_value = record.get("YEAR_")  # Access YEAR_ from each record
        try:
            # Only print the entire record where YEAR_ is 2000 or later
            if int(year_value) >= 2000:
               filtered_records.append(record)
        except (ValueError, TypeError):
            # Skip :ecords where YEAR_ is not a valid number
            continue

df = pd.DataFrame(filtered_records)
df.to_csv('dont_delete/cnra_data.csv', index=False, encoding='utf-8')
# Write the DataFrame to a CSV file
# filter the data by alarm_date

df1 = pd.read_csv('dont_delete/cnra_data.csv') # read the csv file
df1['ALARM_DATE'] = pd.to_datetime(df1['ALARM_DATE'], errors='coerce') # convert the alarm_date column to datetime
#df1['ALARM_DATE'] = df1['ALARM_DATE'].dt.tz_convert('UTC')
df1_sorted = df1.sort_values(by = 'ALARM_DATE', ascending=False) # filter the data by alarm_date
df1_sorted.to_csv('exported_data/cnra_data_sorted.csv', index=False) # save the filtered data to a new csv file



#order by incident_date_created  
# Read the CSV file
df2 = pd.read_csv('dont_delete/fire_ca.csv')
fire = df2[df2['incident_name'].str.contains('Fire', case=False, na=False)].copy()
# copying onlie fire data
# Convert 'incident_date_created' to datetime, coercing errors to NaT
fire['incident_date_created'] = pd.to_datetime(fire['incident_date_created'], errors='coerce')
# Sort by 'incident_date_created' in descending order
df_sorted = fire.sort_values(by='incident_date_created', ascending=False)
# Save the sorted DataFrame to a new CSV file
df_sorted.to_csv('exported_data/sorted_by_year_ca.csv', index=False)

df = pd.read_csv('exported_data/cnra_data_sorted.csv')
    # Remove multiple columns
columns_to_remove = ['OBJECTID', '_id', 'YEAR_','STATE','AGENCY','UNIT_ID','INC_NUM','OBJECTIVE','COMMENTS','IRWINID','FIRE_NUM','DECADES','COMPLEX_ID']
df.drop(columns=columns_to_remove, axis=1, inplace=True)
df.to_csv('exported_data/cnra_data_sorted.csv')

df_sorted.to_csv('exported_data/sorted_by_year_ca.csv', index=False)




# Incident_is_final
# Admin unit 
# incident_control
# incident_cooperating 
# Indicent id 
# Incident url 
# Incident date only 
# Incident date created only 
# Is _active 
# Calfire_incident 
# notfication _desired


df3 = pd.read_csv('exported_data/sorted_by_year_ca.csv')
    # Remove multiple columns
columns_to_remove = ['incident_is_final', 'incident_administrative_unit','incident_administrative_unit_url', 'incident_control','incident_cooperating_agencies','incident_id','incident_url','incident_dateonly_created','is_active','calfire_incident','notification_desired', 'incident_dateonly_extinguished']
df3.drop(columns=columns_to_remove, axis=1, inplace=True)
df3.to_csv('exported_data/sorted_by_year_ca.csv')





