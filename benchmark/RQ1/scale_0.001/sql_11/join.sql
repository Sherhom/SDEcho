drop table flat_table;
create table flat_table as(
	select 
		ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,
		s_name,s_address,s_phone,s_acctbal,s_comment,
		n_nationkey,n_name,n_comment
	from
		nation,supplier,partsupp
	where
		ps_suppkey = s_suppkey and s_nationkey = n_nationkey
);

alter table flat_table
add primary key (ps_partkey,ps_suppkey);
