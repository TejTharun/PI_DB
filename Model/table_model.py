from Exceptions.CreationExceptions import TableAlreadyExists
from typing import Dict
from Utils.TimeUtils import get_time_stamp
import json
import os


class Table:
    def __init__(self, table_name: str, primary_key: str, columns: Dict, partition_size: int = 3,
                 default_time_to_live: int = 10):
        self.primary_key_partitions_directory = None
        self.table_name = table_name
        self.primary_key = primary_key
        self.columns = columns
        self.tables_directory = os.getcwd() + "/Tables"
        self.current_table_path = self.tables_directory + '/' + self.table_name
        self.table_partition_size = partition_size
        self.default_time_to_live = default_time_to_live
        self.meta_data_file_name = self.current_table_path + '/meta_data.json'

    def __all_directories__(self):
        return set([curr_directory for curr_directory in os.listdir(self.tables_directory) if
                    os.path.isdir(os.path.join(self.tables_directory, curr_directory))])

    def fill_meta_data_file_for_table(self):
        meta_data_template_file_path = '/Users/c0t08nm/Documents/PI_DB/Templates/meta_data_template.json'

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

    def create_table(self):
        # check if the table is already present and throw exception if it exists
        try:
            if self.table_name in self.__all_directories__():
                raise TableAlreadyExists("cannot create an existing table", self.table_name)
        except TableAlreadyExists as e:
            print("Error:", e)
            return

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
            self.fill_meta_data_file_for_table()
        except Exception as e:
            print("Error during table creation: ", e)

