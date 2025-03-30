"""
create table <table_name> (primary_key name: string, age: int, class: string) partition_size= 10;

create table first (primary_key name: string, age: int, class: string)
first get table name
"""
from collections import defaultdict
from Model.table_model import Table


class CreateParser:
    def __init__(self, query: str):
        self.query = query
        self.table_name = None
        # primary_key will always be of type string
        self.primary_key = None
        self.columns = defaultdict()
        self.partition_size = None

    def __str__(self):
        return f'{self.table_name} => primary_key is {self.primary_key} and columns are {list(self.columns.items())}'

    def _parse_raw_column_details_(self, raw_colum_details):
        # output will be of format: primary_key name: string, age: int, class: string
        # split it based on ','  then again split each of those outputs based on ":"
        # then get name and type and store it in the columns dict
        raw_colum_details = raw_colum_details.split(',')
        for col_type in raw_colum_details:
            col_name, type = col_type.split(':')
            if 'primary_key' in col_name:
                col_name = col_name.replace('primary_key', '').strip()
                self.primary_key = col_name
            else:
                self.columns[col_name.strip()] = type.strip()

    def parse(self):
        self.query = self.query.lower()
        self.table_name = self.query[self.query.index('table') + len('table'): self.query.index('(')].strip()
        raw_column_details = self.query[self.query.index('(') + 1:self.query.index(')')]
        # print(self.table_name, raw_column_details)
        self._parse_raw_column_details_(raw_column_details)

        print(self)
        partition_size = None
        if 'partition_size' in self.query:
            try:
                partition_size = int(self.query[
                                     self.query.index('partition_size') + len('partition_size='): self.query.index(
                                         ';')].strip())
            except Exception as e:
                print("Error occurred while reading partition size: ", e)
                print("example query:")
                print('\t' + 'create table <table_name> (primary_key name: string, age: int, class: string) '
                             'partition_size= 10;')

        self.partition_size = partition_size
        return self.get_table_model()

    def get_table_model(self):
        if self.partition_size:
            return Table(self.table_name, self.primary_key, self.columns, self.partition_size)
        return Table(self.table_name, self.primary_key, self.columns)
