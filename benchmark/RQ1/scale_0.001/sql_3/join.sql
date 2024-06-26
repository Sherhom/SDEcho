drop table flat_table;
create table flat_table as(
	select lineitem.*,o_orderstatus,o_totalprice,o_orderdate,o_orderpriority,o_clerk,o_shippriority,o_comment,c_custkey,c_name,c_address,c_nationkey,c_phone,c_acctbal,c_mktsegment,c_comment from lineitem,orders,customer where c_custkey = o_custkey and l_orderkey = o_orderkey
);

alter table flat_table
add primary key (L_ORDERKEY,L_LINENUMBER);