import io
import zipfile
import pickle
from io import StringIO,BytesIO
from prefect import flow, task

@task(cache_policy=None)
def unzip_and_decode_files(zip_paths,split_container,db_engine,adls):
	for zip_path in zip_paths:
		file=adls.read_file_in_path(split_container, zip_path)
		data=file['data']
		with io.BytesIO(data) as zip_file:
			with zipfile.ZipFile(zip_file, 'r') as zip_ref:
				with zip_ref.open(zip_ref.namelist()[0]) as file:
					df=pickle.load(file)
					print(f"Unzipped and decoded {zip_path}")
					print(df.shape)
					df.to_sql(name='Flight', con=db_engine, if_exists='append', method=None,index=False)
					print(f"Append {zip_path} to database successfully\n")