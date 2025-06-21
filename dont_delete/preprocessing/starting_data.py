# sort the data by incident_date_created in descending order
# remove the ones that are not fires
# remove unnecessary columns

import pandas as pd


df1 = pd.read_csv('fire_ca.csv')



#keep only items where incident_name contains 'Fire'
fire = df1[df1['incident_name'].str.contains('Fire', case=False, na=False)].copy()# copying onlie fire data
#if incident_date_extinguished is not null, keep it
fire = fire[fire['incident_date_extinguished'].notnull()].copy()  # Keep only rows where 'incident_date_extinguished' is not null
fire['incident_date_created'] = pd.to_datetime(fire['incident_date_created'], errors='coerce')
# Sort by 'incident_date_created' in descending order
df_sorted = fire.sort_values(by='incident_date_created', ascending=False)
# Save the sorted DataFrame to a new CSV file

# colums to remove 
columns_to_remove = [
    'incident_id', 'incident_administrative_unit','incident_date_last_update', 'incident_administrative_unit_url','incident_url',
    'incident_county', 'incident_location','incident_control', 'incident_cooperating_agencies','incident_type','incident_id', 'is_active', 'calfire_incident','notification_desired'
]


df_sorted.drop(columns=columns_to_remove, inplace=True)
# Save the cleaned DataFrame to a new CSV file in dont_delete folder



df_sorted.to_csv('../sorted_by_year.csv', index=False)
