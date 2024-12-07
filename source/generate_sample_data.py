import pandas as pd
import random
import numpy as np
import re
import pickle

from datetime import timedelta
from datetime import datetime
from faker import Faker
from prefect import flow, task
Faker.seed(0)

def convert_numpy_datetime_to_datetime(timestamp):
	unix_epoch = np.datetime64(0, 's')
	one_second = np.timedelta64(1, 's')
	seconds_since_epoch = (timestamp - unix_epoch) / one_second
	timestamp_dt=datetime.fromtimestamp(seconds_since_epoch)
	return timestamp_dt

@task(cache_policy=None)
def generate_sample_flight_data(airport_icao_codes,ac_regs,operator_icao_codes,start_date,end_date,num_rows=10000):
	fake = Faker()
	data = {'ac_reg': [random.choice(ac_regs) for _ in range(num_rows)],
			'flight_start': [fake.date_time_between(start_date=start_date, end_date=end_date) for _ in range(num_rows)],
			'flight_duration_minute': [random.randint(30, 300) for _ in range(num_rows)],
			'airport_from': [random.choice(airport_icao_codes) for _ in range(num_rows)],
			'airport_to': [random.choice(airport_icao_codes) for _ in range(num_rows)],
			'operator_icao_code': [random.choice(operator_icao_codes) for _ in range(num_rows)],
			'ingested_timestamp': [datetime.now().replace(microsecond=0) for _ in range(num_rows)]
	}
			
	
	df = pd.DataFrame(data)
	return df

# For each AC_reg and unique ingested_timestamp, filter and save them as pickle file
@task(cache_policy=None)
def save_df_as_pickle_to_local(df, ac_regs):
	filepaths=[]
	for ac_reg in ac_regs:
		for timestamp in df['ingested_timestamp'].unique():
			filtered_df = df[(df['ac_reg'] == ac_reg) & (df['ingested_timestamp'] == timestamp)]
			operator_icao_code=filtered_df['operator_icao_code'].unique()[0]
			timestamp = convert_numpy_datetime_to_datetime(timestamp)
			
			# Save the filtered DataFrame to a local pickle file
			filename = f"sample_data/{operator_icao_code}_{ac_reg}_{timestamp.strftime('%Y%m%d_%H%M%S')}.pkl"
			filepaths.append(filename)
			filtered_df.to_pickle(filename)
			print(f"Saved {filename}")
	print("Process Done")
	return filepaths

@task(cache_policy=None)
def upload_pickle_files_to_adls(filepaths,raw_container,adls):
	raw_container = "raw"
	adls_filepaths=[]
	for filepath in filepaths:
		metadata={"uploaded_timestamp":datetime.now().replace(microsecond=0).isoformat()}
	
		# Extract the ICAO code from the filename
		icao_regex=r".*([A-Za-z]{3})_.*"
		icao_code=re.match(icao_regex,filepath).group(1)
	
		# Extract the AC registration from the filename
		ac_regex=r".*_(\w{5})_.*"
		ac_reg=re.match(ac_regex,filepath).group(1)
	
		# Extract date from the filename
		date_regex=r".*_(\d{8}).*"
		date_str=re.match(date_regex,filepath).group(1)
	
		# Extract datetime from the filename
		datetime_regex=r".*_(\d{8}_\d{6}).*"
		datetime_str=re.match(datetime_regex,filepath).group(1)
	
		if icao_code and ac_reg and datetime_regex:
			metadata={"uploaded_timestamp":datetime.now().replace(microsecond=0).isoformat()}
			adls_filepath=f"{raw_container}/{icao_code}/{ac_reg}/{date_str}/{datetime_str}.pkl"
			with open(filepath, 'rb') as f:
				df = pickle.load(f)
				df_bytes = pickle.dumps(df)
	
			adls.upload_file_to_container(raw_container, df_bytes, adls_filepath, metadata)
			adls_filepaths.append(adls_filepath)
			print(f"Uploaded {filepath} to ADLS as {adls_filepath} successfully")
		else:
			print(f"Failed to extract ICAO code, AC registration, or timestamp from {filepath}")
	return adls_filepaths