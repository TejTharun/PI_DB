from typing import Dict
from Utils.Constants import get_base_directory
from DDL.Create_Clause import Create_Executor


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

    def create_table(self):
        executor = Create_Executor(self)
        executor.create_table()
