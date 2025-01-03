# [baseline] Use TSExplain for Sequential Explanation Queries

Next, we will introduce how to use `TSExplain` (baseline) for sequential explanation queries.

We provide 11 explanation queries distributed across 4 datasets (TPCH 0.1G / TPCH 0.01G / CMS / NAT).

You can modify the `query configuration file` to determine the `order` and `number` of explanations to search for.


##  Step 1: Compile the TSxplain Program

```
cd build
cmake ../
make
```

**Note**: If a link error occurs, please check the `CMakeList.txt` file.


##  Step 2: Use TSExplain for Sequential Explanation Queries

### 3.1 Explanation Query Configuration File
First, I will introduce the meaning of the query configuration file.

Taking `TPCH_S01/1.txt` as an example:

```
line 1: the data source file
line 3-13: the columns to find explanations
line 14-15: name of groupby column
line 26: k ,the number of explanations you want to get
line 35: order_int,the max order of the explanation
```


**Note**: If you are not familiar with the datasets and the queries being executed, please do not modify query config file.

You can modify `k` to change the number of explanations you want to find.

You can modify `order_int` to change the maximum order of the explanations you want to find.

By default, all programs (including the baseline compared with `SDEcho`) search for 5 explanations with an order not exceeding 8.

That is, `k`=5, `order_int`=8.


###  3.2 Using TSExplain for Sequential Explanation Queries
After compiling the program in the `build` directory,
you can execute the following command:
```
./TSExplain TPCH_S01/1.txt
```
This indicates that you will use TSExplain to complete the explanation query configured in `TPCH_S01/1.txt`.

You can change `the second parameter` of the above command to other explanation queries in the `build` directory, such as `CMS/1.txt`.

In total, we provide 11 explanation queries across 4 datasets.

**Note**: The data and queries we use in TSExplain are the same as that we use in BOExplain/SDEcho,although they are stored differently