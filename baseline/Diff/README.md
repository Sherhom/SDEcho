# [baseline] Use DIff for Sequential Explanation Queries

**NOTE**: The DIFF program is adapted from the code given in the `original DIFF paper`

so that it can perform sequence difference interpretation

Next, we will introduce how to use `DIFF` (baseline) for sequential explanation queries.

We provide 11 explanation queries distributed across 4 datasets (TPCH 0.1G / TPCH 0.01G / CMS / NAT).


##  Step 1 : Compile the DIFF Prgram

Enter the `macrobase/` directory and execute the `build.sh` script to compile the program

**Note**: The `build.sh` script installs the related dependencies. Check if an error occurs

```
cd macrobase/
./build.sh
```

##  Step 2: Generate the corresponding sql statement for sequential explanation queries

Enter the `sql/` directory and use the `generate.sh` script to generate corresponding statements for sequential explanation queries

Make sure that the two parameters followed by the `generate.sh` script are correct

**NOTE**: The first argument followed by the `generate.sh` script must be the file name in the `data/` directory, such as `cms_1.csv`

The second parameter is the highest order of interpretation desired, which defaults to `8` in the previous experiment

for example,

```
cd sql/
./generate.sh cms_1.csv 8
```

**NOTE**: In `data/` , `s01_1.csv` refers to the first query provided on the `TPCH 0.1G` dataset

In `data/` ,`s001_2.csv` refers to the second query provided on the `TPCH 0.01G` dataset




##  Step 3: Execute the explanation query statement 

Make sure you are still in the `macrobase/sql` directory

Execute the following command

```
../bin/macrobase-sql -f result.sql
```

At the end of the terminal output, you can see the cost time and the explanation found

In total, we provide 11 explanation queries across 4 datasets.

**Note**: The data and queries we use in `DIFF` are the same as that we use in `BOExplain/SDEcho`,although they are stored differently
