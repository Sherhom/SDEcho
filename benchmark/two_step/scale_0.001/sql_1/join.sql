drop table if exists flat_table;

create table flat_table as(
	select
		l_orderkey,
		l_partkey,
		l_suppkey,
		l_linenumber,
		l_returnflag,
		l_linestatus,
		l_shipdate,
		l_commitdate,
		l_receiptdate,
		l_shipinstruct,
		l_shipmode
	from
		lineitem
);