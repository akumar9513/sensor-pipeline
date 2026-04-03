import pandas as pd
import os

print('Reading Kaggle IoT dataset...')
df = pd.read_csv('iot_telemetry_data.csv')

df = df.rename(columns={
    'ts':       'timestamp',
    'device':   'sensor_id',
    'temp':     'temperature',
    'humidity': 'humidity',
})

df['timestamp'] = pd.to_datetime(df['timestamp'].astype(float), unit='s')
df['location']  = 'IoT-Lab'
df = df[['sensor_id', 'timestamp', 'location', 'temperature', 'humidity']]

os.makedirs('data', exist_ok=True)
chunk_size = 1000
for i, start in enumerate(range(0, len(df), chunk_size)):
    df[start:start+chunk_size].to_csv(
        f'data/kaggle_iot_batch_{i+1:04d}.csv', index=False
    )

print(f'Done! Created {i+1} CSV files in data/')
