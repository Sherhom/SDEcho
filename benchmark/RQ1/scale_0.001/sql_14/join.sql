drop table flat_table;
create table flat_table as(
	select lineitem.*, p_name,p_mfgr,p_brand,p_type,p_size,p_container,p_retailprice,p_comment
	from lineitem, part
	where l_partkey = p_partkey
);

alter table flat_table
add primary key (L_ORDERKEY,L_LINENUMBER);