# CSVPumper

**Tool to pump data from DB to CSV and from CSV to DB.** 

May be used as second step of DB migration:
1. You need to create same DB schema in target DB
1. Run this tool to load data

Main advantage of this tool: multiple tables support. Useful when you need load data from >10 tables to dedicated CSV files for future loading it to another DB with same schema.
If schema has complex foreign keys, developer should handle it manually: disable/delete them and enable/create afterwords.   

Will read file with tables list and export/import data in parallel. Number of threads are configurable so you can quite fast work with quite big schemas and huge tables up to 2 bil records.
