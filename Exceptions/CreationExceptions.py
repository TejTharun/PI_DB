class TableAlreadyExists(Exception):
    def __init__(self, message, table_name):
        self.message = message
        self.table_name = table_name
        super().__init__(message)

    def __str__(self):
        return f'{self.message} :{self.table_name}'