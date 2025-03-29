from Exceptions.CreationExceptions import TableAlreadyExists
from typing import Dict
from Utils.TimeUtils import get_time_stamp
from Utils.Constants import get_base_directory
import json
import os


class Table:
    def __init__(self, table_name: str, primary_key: str, columns: Dict, partition_size: int = 3,
                 default_time_to_live: int = 10):
        self.primary_key_partitions_directory = None
        self.table_name = table_name
        self.primary_key = primary_key
        self.columns = columns
        self.tables_directory = get_base_directory() + "/Tables"
        self.current_table_path = self.tables_directory + '/' + self.table_name
        self.table_partition_size = partition_size
        self.default_time_to_live = default_time_to_live
        self.meta_data_file_name = self.current_table_path + '/meta_data.json'

    def __all_directories__(self):
        return set([curr_directory for curr_directory in os.listdir(self.tables_directory) if
                    os.path.isdir(os.path.join(self.tables_directory, curr_directory))])

    def fill_meta_data_file_for_table(self):
        meta_data_template_file_path = get_base_directory() + '/Templates/meta_data_template.json'

        template_data = {}
        with open(meta_data_template_file_path, 'r') as fo:
            template_data = json.load(fo)

        if template_data == {}:
            raise Exception("Template data missing")
        template_data['primary_key'] = self.primary_key
        template_data['created_on'] = get_time_stamp()
        template_data['table_name'] = self.table_name
        template_data['columns'] = {}
        for key, value in self.columns.items():
            template_data['columns'][key] = value
        template_data['default_time_to_live_days'] = self.default_time_to_live
        with open(self.meta_data_file_name, 'w') as fo:
            fo.write(json.dumps(template_data))
        print("meta_data created successfully")

    def create_primary_key_partitions(self):
        table_path = self.tables_directory + '/' + self.table_name
        primary_keys_directory = table_path + '/primary_keys'
        self.primary_key_partitions_directory = primary_keys_directory
        try:
            os.makedirs(table_path)
            os.makedirs(self.primary_key_partitions_directory)

            for i in range(self.table_partition_size):
                primary_key_partition_file = self.primary_key_partitions_directory + '/' + str(i) + '.txt'
                with open(primary_key_partition_file, 'w') as fo:
                    pass
            print(f"{self.table_name} and primary_key partitions ({self.table_partition_size})created successfully")
        except Exception as e:
            print("Error during table creation: ", e)
        return

    def create_column_directory(self, columns_directory_name: str, column_name: str):

        current_column_name_file = columns_directory_name + '/' + column_name + '.txt'
        with open(current_column_name_file, 'w') as fo:
            pass

    def create_columns(self):
        columns_directory_name = self.current_table_path + '/Columns'
        try:
            os.makedirs(columns_directory_name)
        except Exception as e:
            print("Error while creating Columns ", e)
        for column in self.columns:
            self.create_column_directory(columns_directory_name, column)
            pass
        print("columns created successfully")

    def create_table(self):
        # check if the table is already present and throw exception if it exists
        try:
            if self.table_name in self.__all_directories__():
                raise TableAlreadyExists("cannot create an existing table", self.table_name)
        except TableAlreadyExists as e:
            print("Error:", e)
            return

        self.create_primary_key_partitions()
        self.fill_meta_data_file_for_table()
        self.create_columns()
