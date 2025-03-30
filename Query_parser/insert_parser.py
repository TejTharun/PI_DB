from collections import defaultdict
from Utils.Constants import get_base_directory
from Model.table_model import Table
from Exceptions.InsertionExceptions import TableNotPresentException, UnknownColumnException, PrimaryAbsentException
from typing import Dict, List, Tuple
import os
import json


class InsertParser:
    def __init__(self, query: str):
        self.query = query
        self.table_name = None
        # primary_key will always be of type string
        self.columns = defaultdict()
        self.partition_size: int = None
        self.insert_if_not_exists: bool = False
        self.column_names_provided: List = None
        self.column_values_provided: List = None
        self.base_path: str = get_base_directory()
        self.meta_data: Dict = None
        self.primary_key_value_pair: Tuple = None

    '''
    INSERT INTO cycling.cyclist_name (id, lastname, firstname) 
   VALUES (c4b65263-fe58-4846-83e8-f0e1c13d518f, 'RATTO', 'Rissella') 
IF NOT EXISTS; 
    '''

    def parse(self):
        try:
            self.query = self.query.lower()
            self.table_name = self.query[self.query.index('into') + len('into'): self.query.index('(')].strip()
            raw_columns = self.query[self.query.index('(') + 1:self.query.index(')')].split(',')
            raw_columns = [column.strip() for column in raw_columns]
            self.query = self.query[self.query.index('values') + len('values'):].strip()
            column_values = self.query[1:-1]  # remove '(' ')' at the ends
            column_values = column_values.split(',')
            column_values = [col_val.strip() for col_val in column_values]
            self.column_names_provided = raw_columns
            self.column_values_provided = column_values


        except Exception as e:
            print(f"Error in parsing INSERT Query {self.query} ", e)

        try:
            self.validate_table_name()
            self.form_meta_data()
            self.validate_column_names()
            self.validate_column_values_data_type()

        except TableNotPresentException as tableAbsentException:
            print(tableAbsentException)
        except UnknownColumnException as unknownColumn:
            print(unknownColumn)
        except Exception as e:
            print("Some error occurred", e)

        return self.get_table_model()
    def form_meta_data(self):
        table_meta_data_file_path = self.base_path + '/Tables/' + self.table_name + '/meta_data.json'
        with open(table_meta_data_file_path, 'r') as fo:
            meta_data = json.load(fo)
        self.meta_data = meta_data

    def validate_table_name(self):
        tables_directory = self.base_path + '/Tables'
        is_table_present = self.table_name in os.listdir(tables_directory)
        if not is_table_present:
            raise TableNotPresentException("Table must be created before insertion")

    def validate_column_names(self):
        for name in self.column_names_provided:
            if name == self.meta_data['primary_key']:
                continue
            if name not in self.meta_data['columns']:
                message = 'column must be present in ' + ','.join([col for col in self.meta_data['columns']])
                raise UnknownColumnException(message, name, self.table_name)

    def validate_column_values_data_type(self):
        if len(self.column_names_provided)+1 != self.column_values_provided:
            raise Exception("Column name and Values provided must be same")
        table_meta_data_file_path = self.base_path + '/' + self.table_name + '/meta_data.json'
        with open(table_meta_data_file_path, 'r') as fo:
            meta_data = json.load(fo)
        primary_key_value_pair = [(col, val) for col, val in
                                  zip(self.column_names_provided, self.column_values_provided) if col == meta_data][0]
        if not primary_key_value_pair:
            raise PrimaryAbsentException('', self.meta_data['primary_key'])

        self.primary_key_value_pair = primary_key_value_pair
        if 'str' not in type(primary_key_value_pair[1]):
            raise Exception('primary key must be of type str')
        column_value_pairs = [(col, val) for col, val in zip(self.column_names_provided, self.column_values_provided)]
        for col, val in column_value_pairs:
            if self.validate_col_value_pair(col, val):
                raise Exception(f"{col}'s {val} must be of type {self.meta_data['columns'][col]}")

        self.columns = {col: val for col, val in column_value_pairs}

    def validate_col_value_pair(self, col, val):
        col_data_type = self.meta_data['columns'][col]
        try:
            if 'str' == col_data_type:
                val = str(val)
            elif 'int' == col_data_type:
                val = int(val)

            elif 'bool' == col_data_type:
                val = bool(val)
            else:
                # only float is left in data types we support
                val = float(val)
        except Exception as e:
            return False

        return True



    def get_table_model(self):
        return Table.initialize_table_for_insertion(self.table_name, self.primary_key_value_pair, self.columns)
