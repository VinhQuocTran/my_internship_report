
import os
import math
import yaml

from datetime import date
from prefect import flow, task
from generate_sample_data import *
from splitter import *
from zipped_raw import *
from unzip_decode_to_db import *
from utils import adls_module

import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

@flow
def aviation_etl():
	# Create credentials 
	with open("credentials.yaml", "r") as config_file:
		config = yaml.safe_load(config_file)
		
	adls = adls_module.ADLSModule(config['adls']['sa_name'], config['adls']['connection_string'], config['adls']['key'])

	driver = "{ODBC Driver 18 for SQL Server}"
	server = config['db']['server']
	database = config['db']['database']
	user = config['db']['user']
	password = config['db']['password']
 
	connection_string = f"Driver={driver};SERVER={server};DATABASE={database};UID={user};PWD={password}"
 
	#### Step 1
	ac_regs=[]
	for i in range(10):
		ac_reg="AB"+str(i)*3
		ac_regs.append(ac_reg)
		
	ac_regs.append("BO123")

	airport_icao_codes=["VVNB",'VVTS','VDAD','VVCR','VVNB','VVTS','VDAD','VVCR','VVNB','VVTS']
	operator_icao_codes=['VNA','VJC','BAV']
	start_date=date.fromisoformat('2024-11-01')
	end_date=date.fromisoformat('2024-11-15')
	num_rows=5000
	df=generate_sample_flight_data(airport_icao_codes,ac_regs,operator_icao_codes,start_date,end_date,num_rows=num_rows)
	print("Generated sample data")
 
	#### Step 2
	filepaths=save_df_as_pickle_to_local(df, ac_regs)
	print("Saved sample data as pickle files")
 
	#### Step 3
	adls_filepath=upload_pickle_files_to_adls(filepaths,"raw",adls)
	print("Uploaded pickle files to ADLS")
 
	#### Step 4
	raw_container = "raw"
	zip_container = "zipped-raw"
	zip_paths=zip_and_upload_files_to_adls(adls_filepath,raw_container,zip_container,adls)
	print("Zipped and uploaded files to ADLS")
 
	#### Step 5
	manufacture_mapping={}
	for ac_reg in ac_regs:
		if ac_reg!="BO123":
			manufacture_mapping[ac_reg]="AIRBUS"
		else:
			manufacture_mapping[ac_reg]="NON-AIRBUS"

	zip_container="zipped-raw"
	airbus_container="airbus-raw"
	non_airbus_container="non-airbus-raw"
	airbus_zip_paths,non_airbus_zip_paths=split_data_by_manufacturer(zip_paths,manufacture_mapping,zip_container,airbus_container,non_airbus_container,adls)
	print("Split data by manufacturer successfully")
 
	#### Step 6
	connection_url = URL.create(
		"mssql+pyodbc", 
		query={"odbc_connect": connection_string}
	)
	airbus_raw_container="airbus-raw"
	non_airbus_raw_container="non-airbus-raw"
 
	engine = create_engine(connection_url,fast_executemany=True)
	unzip_and_decode_files(airbus_zip_paths,airbus_raw_container,engine,adls)
	unzip_and_decode_files(non_airbus_zip_paths,non_airbus_raw_container,engine,adls)
	print("Unzipped and decoded files to database successfully")
 
 
if __name__ == "__main__":
    aviation_etl()
 
	

    
    
    
    
    



