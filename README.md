# SDEcho: Efficient Explanation of Aggregated Sequence Difference 

**Note:** This project is currently being updated. Please check back later for the latest version.

We introduced a novel framework for explanation searching, SDEcho, which addresses the challenge of explaining aggregated sequence differences. SDEcho employs pruning techniques from three perspectives – pattern, order, and dimension – to ensure concise and accurate explanations while considering the characteristics of each pruning algorithm for hybrid optimization. 

## Directory Layout

- `baselines/` - contains all pruning method baselines mentioned in the paper.

- `benchmark/` - contains all the files necessary to the implementation related to benchmark generation and explanation confidence evaluation mentioned in the paper.

- `data/` - contains the formated data and queries used for efficiency evaluation for example.

- `src/` - contains all the files necessary to the implementation of SDEcho.

## Dataset
```
#Use the following sql to import the data in "data/mini_sample_data" into the database:
copy table_name from file_name with delimiter as ',' NULL ''; 

#for example:
copy nat2021_01 from 'data/mini_sample_data/nat2021_01' with delimiter as ',' NULL ''; 

#Configure database information in "src/main.cpp", which are "port", "dbname", "user" and "password".

```


## SDEcho Compiling and Running

```
# compile
cd src
mkdir build
cd build
cmake ..
make

# run
cd src
sh test.sh

```

## Baselines Compiling and Running
```
#All baselines are compiled and run in the same way as SDEcho.

```


## Benchmark
```
#Initialize three databases from TPC-H: 'origin' for original data, 'tpch_rq1' for data after insertion of patterns and 'noise' where the inserted patterns are selected from. Notice that databases 'origin' and 'tpch_rq1' share the same scale while the scale of 'noise' should be larger. 
#We provide data sets of different scales under "./benchmark/TPC-H".
#Use the following sqls to import data, change the scale size to import TPC-H data at different scales. 
copy region from './benchmark/TPC-H/scale_0.001/region.tbl' with delimiter as '|' NULL '';
copy nation from './benchmark/TPC-H/scale_0.001/nation.tbl' with delimiter as '|' NULL '';
copy partsupp from './benchmark/TPC-H/scale_0.001/partsupp.tbl' with delimiter as '|' NULL '';
copy customer from './benchmark/TPC-H/scale_0.001/customer.tbl' with delimiter as '|' NULL '';
copy lineitem from './benchmark/TPC-H/scale_0.001/lineitem.tbl' with delimiter as '|' NULL '';
copy orders from './benchmark/TPC-H/scale_0.001/orders.tbl' with delimiter as '|' NULL '';
copy part from './benchmark/TPC-H/scale_0.001/part.tbl' with delimiter as '|' NULL '';
copy supplier from './benchmark/TPC-H/scale_0.001/supplier.tbl' with delimiter as '|' NULL ''; 

```
```
#Modify the database connection information in the file 'benchmark/RQ1/connect.py'
#generate inserted patterns for a specific sql
python benchmark/RQ1/dbgen.py

```

More datasets of `large scales` can be downloaded from https://drive.google.com/drive/folders/1CDlnt1sLHbh0JJXlH7Y4wyHR2rKCT_QK?usp=sharing


## Explanation Confidence Evaluation

```
python benchmark/RQ1/explainEngine.py

```


