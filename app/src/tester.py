""" Test File Read operation
"""
import loader as ld
import reader as rd


import os
import sys
import datetime
import logging
import pandas as pd
import numpy as np
import zipfile
import io
import glob
import typing



def read_ingestion_report(ingested_file:str, report_dir:str) -> dict:
	ingested_file_pattern = f'{ingested_file}_*.txt'
	report_files = glob.glob(os.path.join(report_dir,ingested_file_pattern))
	sorted_files = sorted(report_files, key=os.path.getctime)
	newest_report =  open(sorted_files[-1], "r").readlines()
	counts = {'origination':{},'performance':{}}
	for line in newest_report:
		if line.startswith('Per') or line.startswith('Ori'):
			split = line.strip("\n\r").split('=')
			sub_split = split[0].split(" ")

			counts[sub_split[0].lower()][sub_split[-1]] = split[1]
	return counts

def compare_report(ingestion_counts:dict, test_counts:dict) -> None:
	for table,table_data in ingestion_counts.items():
		db_counts = test_counts[table]
		for quarter, quarter_count in table_data.items():
			records_in_file = int(quarter_count)
			records_in_db = int(db_counts[db_counts['src_file']==quarter]["file_count"].iloc[0])
			test_result = "Ingestion successful" if records_in_file == records_in_db else "Ingestion unsuccessful."
			print (f"In {quarter}, Records in file: {records_in_file:,}.\tRecords in db: {records_in_db:,}.\t{test_result}")


if __name__ == '__main__':
	root_dir = os.path.dirname(os.path.dirname(__file__))
	log_file_buffer = os.path.join(
		root_dir, 'log', f'sfl_loan_test_{datetime.datetime.now().date()}.log'
	)
	logging.basicConfig(
		filename=log_file_buffer,
		filemode='w', 
		level='DEBUG', 
		format='\n\n%(asctime)s \t\t%(levelname)s \t\t%(name)s \t\t%(message)s'
	)
	LOGGER = logging.getLogger(__name__)


	db_config = sys.argv[1]
	data_loader = ld.Loader(db_config)

	try:
		data_path = sys.argv[2]
		file_reader = rd.Reader(data_loader)
		sub_filename = data_path.split("/")[-1]
		report_file = os.path.join(
			root_dir, 'report',
			f'{sub_filename}_test_{{}}_{datetime.datetime.now().strftime("%Y-%m-%d-%H%M%S")}.csv'
		)

		test_counts = file_reader.db_row_check(data_path, report_file)
		ingestion_counts = read_ingestion_report(sub_filename, os.path.join(root_dir,'report'))
		compare_report(ingestion_counts, test_counts)

	except Exception:
		print(f"\n\nAn error occurred. Please check the logs for more information.")
		LOGGER.debug(f'  An error occurred.', exc_info=True)
		sys.exit(1)
	else:
		print(f"\nFinished testing file: {sub_filename}")
	finally:
		data_loader.end_session()

