#  Use SDEcho for Sequential Explanation Queries

Next, we will introduce how to use `SDEcho` for sequential explanation queries.

We provide 11 explanation queries distributed across 4 datasets (TPCH 0.1G / TPCH 0.01G / CMS / NAT).

You can modify the `query configuration file` to determine the `order` and `number` of explanations to search for.

##  Step 1 : Modify the Database Configuration File and CMakeList.txt File

### 1.1 Modify the Database Configuration File
You need to modify the `hostaddr`/`port`/`dbname`/`username`/`password` fields in the `./build/db_config.txt` file,
so that the `SDEcho` program can correctly connect to the database and read the data.

If you have not modified the `dbname` in the `data import` section, it should be `sdecho`.

### 1.2 Modify the CMakeList.txt File
You need to modify `line 6` in the `./CMakeList.txt` file and set it to the `root directory` of `PostgreSQL`,
so that the `SDEcho` program can correctly use the interfaces provided by PostgreSQL.


##  Step 2: Compile the SDEcho Program

```
cd build
cmake ../
make
```

**Note**: If a link error occurs, please check the `CMakeList.txt` file.


##  Step 3: Use SDEcho for Sequential Explanation Queries

### 3.1 Explanation Query Configuration File
First, I will introduce the meaning of the query configuration file.

Taking `TPCH_S01/1.txt` as an example:

```
line 1: focusColNames ;         //the columns to find explanations
line 2: table1Name ;			//name of the first table
line 3: table2Name ;			//name of the second table
line 4: groupbyColName;			//name of groupby colum
line 5: aggColName;				//name of aggragated column
line 6: aggFunc;		        //aggragate function name: !!count sum!! only now
line 7: k ;					    //the number of explanations you want to get
line 8: order_int ;				//the max order of the explanation
```

This indicates that you want to find `k` explanations of the highest order `order_int` on the attribute set listed in `focusColNames` for the two sequences obtained from the following two queries:

```
SELECT aggFunc(aggColName) FROM table1Name GROUP BY groupbyColName;
SELECT aggFunc(aggColName) FROM table2Name GROUP BY groupbyColName;
```

**Note**: If you are not familiar with the datasets and the queries being executed, please do not modify `lines 1-6`.

You can modify `line 7` to change the number of explanations you want to find.

You can modify `line 8` to change the maximum order of the explanations you want to find.

By default, all programs (including the baseline compared with `SDEcho`) search for 5 explanations with an order not exceeding 8.

That is, `k`=5, `order_int`=8.


###  3.2 Using SDEcho for Sequential Explanation Queries
After compiling the program in the `build` directory,
you can execute the following command:
```
./SDEcho TPCH_S01/1.txt
```
This indicates that you will use SDEcho to complete the explanation query configured in `TPCH_S01/1.txt`.

You can change `the second parameter` of the above command to other explanation queries in the `build` directory, such as `CMS/cms_1.txt`.

In total, we provide 11 explanation queries across 4 datasets.



