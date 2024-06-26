select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)),
	o_orderdate,
	o_shippriority
from
	flat_table
where
	c_mktsegment = 'BUILDING'
	and o_orderdate < date '1995-03-15'
	and l_shipdate > date '1995-03-15'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority

