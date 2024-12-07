import io
import zipfile
from io import StringIO,BytesIO
from prefect import flow, task


@task(cache_policy=None)
def zip_and_upload_files_to_adls(adls_filepaths,raw_container,zip_container,adls):
	zip_paths=[]
	for raw_path in adls_filepaths:
		curr_file=adls.read_files_in_path(raw_container, raw_path)

		with io.BytesIO() as buffer:
			with zipfile.ZipFile(buffer, 'w') as zipf:
				zipf.writestr(raw_path, curr_file[0]['data'])
			data=buffer.getvalue()
   
		zip_path=raw_path+".zip"
		adls.upload_file_to_container(zip_container,data,zip_path)
		zip_paths.append(zip_path)
		print(f"Uploaded {raw_path} to {zip_container} successfully")
	print("Process Done")
	return zip_paths