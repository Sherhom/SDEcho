select data_table.*, l_linestatus as aggcol from data_table where c_mktsegment = 1;
select data_table.*, l_linestatus as aggcol from data_table where c_mktsegment = -1;