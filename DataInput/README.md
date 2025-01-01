# Data Import

In this section, we will guide you through importing the data required for the SDEcho experiment in two steps:

1. Creating the database and tables 

2. Importing data into the tables.
 
**NOTE**: Our experimental data needs to be stored in `PostgreSQL`, so you should have `PostgreSQL` installed

##  1. Creating the Database and Tables

You can run the following terminal command to create the relevant database and tables required for the SDEcho implementation.

You need to modify the three parameters: `YOUR_DB_PORT`/`YOUR_DB_USERNAME`/`TMEP_DB`.

```
psql -p YOUR_DB_PORT -U YOUR_DB_USERNAME -d TMEP_DB -f table_create.sql
```

`table_create.sql`: This will create a database named `sdecho`, switch to this database, and then create the necessary tables within it. So `TMEP_DB` in the above terminal command can be set to any existing database in your system .

##  2. Importing Data into Tables

first of all , you need to `unzip` the `cms_2021.zip` and `cms_2022.zip` in the directory `CSVdata`

```
unzip cms_2021.zip
unzip cms_2022.zip
```

You can run the following terminal command to import the experiment-related data in the directory `CSVdata` into the previously created tables.

You need to modify the two parameters: `YOUR_DB_PORT`/`YOUR_DB_USERNAME`.

**Note** : You need to modify the paths in the `data_input.sql` file so that the script can correctly locate the CSV files in the subdirectory `CSVdata`.

```
psql -p YOUR_DB_PORT -U YOUR_DB_USERNAME -d sdecho -f data_input.sql
```

`data_input.sql`: This will import the CSV files from the CSVdata directory into the tables that were just created.