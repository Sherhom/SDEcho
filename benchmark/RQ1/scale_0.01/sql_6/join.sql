drop table flat_table;
create table flat_table as(
	select * from lineitem
);

alter table flat_table
add primary key (L_ORDERKEY,L_LINENUMBER);