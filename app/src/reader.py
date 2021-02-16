"""Read zipped file into database
"""
import data_dictionary as dt
from loader import Loader

import os
import sys
import datetime
import logging
import pandas as pd
import numpy as np
from zipfile import ZipFile
import io
import typing


chunk_size = 50000

class Reader:
	def __init__(self, data_loader:Loader) -> None:
		self.origination_count = {}
		self.performance_count = {}
		self.data_schema = dt.data_dict
		self.loader = data_loader


	def read_zip(self, file_path:str) -> None:
		# Read zipped file
		zipped_file = ZipFile(file_path, "r")
		print("\nFile being read: ")
		zipped_file.printdir()

		for each_quarter in zipped_file.namelist():
			quarterly_zipped_file = ZipFile(io.BytesIO(zipped_file.read(each_quarter)))
			print("\nFile being read: ")
			quarterly_zipped_file.printdir()
			
			for loan_file in quarterly_zipped_file.namelist():
				self.read_csv(each_quarter, quarterly_zipped_file, loan_file)


	def read_csv(self, parent_file:str, parent_zip:ZipFile, child_zip:str) -> None:
		# Read csv file in chunks
		quarter = parent_file.split(".")[0].split("_")[-1]
		if quarter not in self.origination_count:
			self.origination_count[quarter] = 0
		if quarter not in self.performance_count:
			self.performance_count[quarter] = 0

		df_iterable = pd.read_csv(
			parent_zip.open(child_zip), compression='infer',
			chunksize=chunk_size, sep="|", header=None
		)
		for df in df_iterable:
			if child_zip.find("time") == -1:
				self.origination_count[quarter] += df.shape[0]
				table = 'origination'
			else:
				self.performance_count[quarter] += df.shape[0]
				table = 'performance'
			df['src_file'] = quarter
			self.load_db(df, table)
			


	def load_db(self, df:pd.DataFrame, table:str) -> None:
		# Load data into database
		df = df.where(pd.notnull(df), None)
		arg = self.data_schema[table].copy()
		arg['table'] = table
		arg['data'] = df.to_numpy()
		self.loader.insert(arg)


	def write_read_report(self, ingested_file:str, report_file:str) -> None:
		# Write record count report after reading data files
		file_year = ingested_file.split("/")[-1].split(".")[0].split("_")[-1]
		origination_quarters = sorted([x for x in self.origination_count])
		origination_string = '\n'.join([
			f'Origination count {x}={self.origination_count[x]}' for x in origination_quarters
		])
		performance_quarters = sorted([x for x in self.performance_count])
		performance_string = '\n'.join([
			f'Performance count {x}={self.performance_count[x]}' for x in performance_quarters
		])
		report_content = f"""Ingested file={ingested_file}\nFile year={file_year}\n{origination_string}\n{performance_string}"""
		with open(report_file,'w') as writer:
			writer.write(report_content)


	def db_row_check(self, ingested_file:str, report_file:str) -> dict:
		# Test data insertion accuracy
		file_year = ingested_file.split("/")[-1].split(".")[0].split("_")[-1]
		test_counts = {}
		for table in ['origination', 'performance']:
			df = self.loader.select({
				'table':table
				,'from_clause': table
				, 'select_clause': f"'{table}' As table_name, src_file, COUNT(*) AS file_count"
				, 'where_clause': f"src_file LIKE '%{file_year}' "
				,'group_clause':'table_name, src_file'
			})
			df.to_csv(report_file.format(table), index=False)
			test_counts[table] = df.copy()
		return test_counts







if __name__ == '__main__':
	root_dir = os.path.dirname(os.path.dirname(__file__))
	log_file_buffer = os.path.join(
		root_dir, 'log', f'sfl_loan_{datetime.datetime.now().date()}.log'
	)
	logging.basicConfig(
		filename=log_file_buffer,
		filemode='w', 
		level='DEBUG', 
		format='\n\n%(asctime)s \t\t%(levelname)s \t\t%(name)s \t\t%(message)s'
	)
	LOGGER = logging.getLogger(__name__)


	db_config = sys.argv[1]
	data_loader = Loader(db_config)

	try:
		for k, v in dt.data_dict.items():
			val = v.copy()
			val['table'] = k
			data_loader.terraform(val)

		data_path = sys.argv[2]
		file_reader = Reader(data_loader)
		file_reader.read_zip(data_path)

		sub_filename = data_path.split("/")[-1]
		report_file = os.path.join(
			root_dir, 'report',
			f'{sub_filename}_{datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")}.txt'
		)
		file_reader.write_read_report(data_path, report_file)
	except Exception:
		print(f"\n\nAn error occurred. Please check the logs for more information.")
		LOGGER.debug(f'  An error occurred.', exc_info=True)
		sys.exit(1)
	else:
		print(f"\nFinished loading file: {sub_filename}")
	finally:
		data_loader.end_session()

