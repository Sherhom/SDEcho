-- using default substitutions


select
	l_orderkey,
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date '1994-01-01'
	and l_shipdate < date '1994-01-01' + interval '1' year
	and l_discount between .06 - 0.01 and .06 + 0.01
	and l_quantity < 24
group by l_orderkey;
set rowcount -1
go

