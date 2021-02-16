"""Simplfied ORM for interacting with database.
"""

import sys
import logging
import pandas as pd
import numpy as np
import psycopg2
import typing


LOGGER = logging.getLogger(__name__)



class Loader(object):
	def __init__(self, env_file:str) -> None:
		self.__get_connection(env_file)
	

	def __get_connection(self, env_file:str) -> None:
		key_arr = open(env_file, "r").readlines()
		args = {}
		for line in key_arr:
			split = line.strip().split('=')
			args[split[0]] = split[1]

		try:
			self.__connection = psycopg2.connect(**args)
		except(Exception, psycopg2.DatabaseError) as error:
			print("Unable to connect to DB. Please check logs for more information")
			LOGGER.debug(f"Unable to connect to DB", exc_info=True)
			sys.exit(1)


	def terraform(self,arg:dict) -> None:
		# Ensure table exists before inserting data into database
		column_definition = ''', '''.join([
			f'''  "{k.lower()}" {arg['data_types'][k]}  ''' for k in arg['fields']
		])
		primary_keys = ','.join([f'"{x}"' for x in arg['primary_keys']])


		#  Get create query for table
		sql_query = f''' CREATE TABLE IF NOT EXISTS {arg['table']} ({column_definition}, PRIMARY KEY ({primary_keys})); '''
		try:
			cursor = self.__connection.cursor()
			cursor.execute(sql_query)
			self.__connection.commit()
		except(Exception, psycopg2.DatabaseError) as error:
			self.__connection.rollback()
			LOGGER.debug(f'  Error creating table {arg["table"]}',exc_info=True)
			raise
		finally:
			cursor.close()


	def insert(self,arg:dict) -> None:
		# Insert records into table
		db_columns = [ ''' "{}" '''.format(x.lower()) for x in arg['fields'] ]
		not_pk_columns = [
			f'"{x.lower()}"'
			for x in arg['fields'] if x.lower() not in arg['primary_keys']
		]


		data = np.array(arg['data'])
		rows = data.shape[0]
		insert_values = tuple(tuple(x) for x in data)
		insert_arg = {
			'table':arg['table']
			,'primary_keys':','.join([f'"{x}"' for x in arg['primary_keys']])
			,'values':',\n\t'.join(['%s']*rows)
			,'conflict_insert_columns': ','.join(not_pk_columns)
			,'exclude_columns': ','.join([f'EXCLUDED.{x}' for x in not_pk_columns])
			,'insert_columns':','.join(db_columns)
		}

		sql_query = '''
				INSERT INTO 
					{table}({insert_columns}) 
				VALUES 
					{values}
				ON CONFLICT
					({primary_keys}) 
				DO UPDATE SET 
					({conflict_insert_columns})=({exclude_columns});
			'''.format(**insert_arg)

		try:
			cursor = self.__connection.cursor()
			cursor.execute(sql_query, insert_values)
			self.__connection.commit()
		except(Exception, psycopg2.DatabaseError) as error:
			self.__connection.rollback()
			LOGGER.debug(f'  Error inserting into table {arg["table"]}',exc_info=True)
			raise
		finally:
			cursor.close()


	def select(self,arg:dict) -> pd.DataFrame:
		# Retrieve data from database
		sql_query=''' SELECT {select_clause} FROM {from_clause} '''.format(**arg)

		if 'where_clause' in arg:
			sql_query +='''  WHERE {where_clause} '''.format(**arg)
		
		if 'group_clause' in arg:
			sql_query +=''' GROUP BY {group_clause} '''.format(**arg)
			
			if 'having_clause' in arg:
				sql_query +=''' HAVING {having_clause} '''.format(**arg)
		
		if 'order_clause' in arg:
			sql_query +=''' ORDER BY {order_clause} '''.format(**arg)
		
		if 'limit_clause' in arg:
			sql_query +=''' LIMIT {limit_clause} '''.format(**arg)
		
		sql_query +=''';'''


		try:
			cursor = self.__connection.cursor()
			cursor.execute(sql_query)
			data = np.array(cursor.fetchall())
			columns = [x[0] for x in cursor.description]
			return pd.DataFrame(data, columns=columns)
		except(Exception, psycopg2.DatabaseError) as error:
			self.__connection.rollback()
			LOGGER.debug(f'  Error selecting from table {arg["table"]}',exc_info=True)
			raise
		finally:
			cursor.close()


	def end_session(self):
		self.__connection.close()
