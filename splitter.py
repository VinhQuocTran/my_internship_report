import re
from prefect import flow, task

@task(cache_policy=None)
def split_data_by_manufacturer(zip_paths,manufacture_mapping,zip_container,airbus_container,non_airbus_container,adls):
	airbus_zip_paths=[]
	non_airbus_zip_paths=[]
	for zip_path in zip_paths:
		ac_regex=r".*/(\w{5})/.*"
		ac_reg=re.match(ac_regex,zip_path).group(1)
		
		if ac_reg and ac_reg in manufacture_mapping and manufacture_mapping[ac_reg]=="AIRBUS":
			destination_container='airbus-raw'
			airbus_zip_paths.append(zip_path)
		else:
			destination_container='non-airbus-raw'
			non_airbus_zip_paths.append(zip_path)
	  
		adls.copy_file_between_container(zip_container, zip_path, destination_container, zip_path)
	return airbus_zip_paths,non_airbus_zip_paths