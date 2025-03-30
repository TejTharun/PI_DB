from Query_parser.create_parser import CreateParser
from Query_parser.insert_parser import InsertParser

# query = 'create table Students (primary_key id: string, age: int, class: str, section: str);'
# table_object = CreateParser(query).parse()
#
# table_object.create_table()


'''
    INSERT INTO cycling.cyclist_name (id, lastname, firstname) 
   VALUES (c4b65263-fe58-4846-83e8-f0e1c13d518f, 'RATTO', 'Rissella') 
IF NOT EXISTS; 
'''
insert_query = "insert into students (id, age, class, section) values ('Tej', 25, 'Tenth', 'A')"
table_object = InsertParser(query=insert_query).parse()
table_object.insert_data()
