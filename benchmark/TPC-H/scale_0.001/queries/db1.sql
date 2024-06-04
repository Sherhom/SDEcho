-- using default substitutions


select
	l_returnflag,
	l_shipdate,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval 30 day and l_returnflag = 'A' or l_returnflag = 'R'
group by
	l_returnflag,
	l_shipdate
order by
	l_shipdate;
set rowcount -1
go

