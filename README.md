# CSVPumper

**Tool to pump data from DB to CSV and from CSV to DB.** 

Main advantage of this tool: multiple tables support. Useful when you need load data from >10 tables to dedicated CSV files for future loading it to another DB with same schema. 

Will read file with tables list and export/import data in parallel. Number of threads are configurable so you can quite fast work with quite big schemas and huge tables up to 2 bil records.
