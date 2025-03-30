class TableNotPresentException(Exception):
    def __init__(self, message, table_name):
        self.message = message
        self.table_name = table_name
        super().__init__(message)

    def __str__(self):
        return f'{self.table_name} is not present in DB, {self.message}'


class UnknownColumnException(Exception):
    def __init__(self, message, column_name, table_name):
        self.message = message
        self.column_name = column_name
        self.table_name = table_name
        super().__init__(message)

    def __str__(self):
        return f'{self.column_name} not present in {self.table_name}. {self.message}'


class PrimaryAbsentException(Exception):
    def __init__(self, message, primary_key):
        self.primary_key = primary_key
        super().__init__(message)

    def __str__(self):
        return f'{self.primary_key} must be present'
