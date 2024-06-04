drop table flat_table;
create table flat_table as(
	select 
		ps_partkey,ps_suppkey,ps_availqty,ps_supplycost,ps_comment,
		p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment
	from
		partsupp,part
	where
		ps_suppkey = p_partkey
);

alter table flat_table
add primary key (ps_partkey,ps_suppkey);
