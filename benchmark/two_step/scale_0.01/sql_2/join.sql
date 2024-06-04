drop table flat_table;

create table flat_table as(
	select
		l_orderkey,
		l_partkey,
		l_suppkey,
		l_returnflag,
		l_linestatus,
		l_shipinstruct,
		l_shipmode,
		o_orderstatus,
		o_orderdate,
		o_orderpriority,
		o_shippriority,
		c_custkey,
		c_nationkey,
		c_mktsegment
	from
		lineitem,
		orders,
		customer
	where
		c_custkey = o_custkey
		and l_orderkey = o_orderkey
);