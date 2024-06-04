select data_table.*, l_returnflag as aggcol from data_table where l_shipmode = 1;
select data_table.*, l_returnflag as aggcol from data_table where l_shipmode = -1;