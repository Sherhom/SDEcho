--create database
CREATE DATABASE sdecho;

\c sdecho;

--create table for TPCH scale=0.1

CREATE TABLE public.s01_sql1_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql1_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql1_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql1_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql2_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql2_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql3_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql3_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql2_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql2_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql3_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql3_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql4_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    s_nationkey character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql4_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    s_nationkey character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql5_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql5_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql6_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql6_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql7_0 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    o_custkey character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql7_1 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    o_custkey character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql8_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    p_container character varying,
    p_size character varying,
    l_quantity character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql8_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    p_container character varying,
    p_size character varying,
    l_quantity character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql9_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql9_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql10_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql10_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql11_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql11_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql12_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql12_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql13_0 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql13_1 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql14_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s01_sql14_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    groupcol integer,
    aggcol double precision
);

--create table for TPCH scale=0.01

CREATE TABLE public.s001_sql1_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql1_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql1_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql1_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql2_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql2_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql3_0_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql3_1_x (
    o_orderstatus character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    o_orderdate character varying,
    l_shipinstruct character varying,
    l_commitdate character varying,
    cn_regionkey character varying,
    l_shipdate character varying,
    c_mktsegment character varying,
    l_receiptdate character varying,
    l_shipmode character varying,
    p_retailprice character varying,
    l_linenumber character varying,
    l_tax character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql2_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql2_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql3_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql3_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql4_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    s_nationkey character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql4_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    s_nationkey character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql5_0 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql5_1 (
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    l_shipdate character varying,
    l_receiptdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql6_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql6_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql7_0 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    o_custkey character varying,
    groupcol integer,
    aggcol double precision
);


CREATE TABLE public.s001_sql7_1 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    o_custkey character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql8_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    p_container character varying,
    p_size character varying,
    l_quantity character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql8_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    p_container character varying,
    p_size character varying,
    l_quantity character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql9_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql9_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    o_orderdate character varying,
    l_commitdate character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql10_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql10_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql11_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql11_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql12_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql12_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    p_container character varying,
    p_size character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql13_0 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql13_1 (
    o_orderstatus character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    c_nationkey character varying,
    o_clerk character varying,
    o_orderdate character varying,
    c_acctbal character varying,
    groupcol integer,
    aggcol double precision
);


CREATE TABLE public.s001_sql14_0 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    groupcol integer,
    aggcol double precision
);

CREATE TABLE public.s001_sql14_1 (
    l_linestatus character varying,
    o_orderstatus character varying,
    l_returnflag character varying,
    l_shipinstruct character varying,
    sn_regionkey character varying,
    p_mfgr character varying,
    o_orderpriority character varying,
    cn_regionkey character varying,
    c_mktsegment character varying,
    l_shipmode character varying,
    l_linenumber character varying,
    l_tax character varying,
    l_discount character varying,
    s_nationkey character varying,
    p_brand character varying,
    c_nationkey character varying,
    groupcol integer,
    aggcol double precision
);


--create table for NAT dataset 

CREATE TABLE public.nat2021_x_1 (
    group1 integer,
    group2 integer,
    group3 integer,
    c01 character varying,
    c02 character varying,
    c03 character varying,
    c04 character varying,
    c05 character varying,
    c06 character varying,
    c07 character varying,
    c08 character varying,
    c09 character varying,
    c10 character varying,
    c11 character varying,
    c12 character varying,
    c13 character varying
);


CREATE TABLE public.nat2022_x_1 (
    group1 integer,
    group2 integer,
    group3 integer,
    c01 character varying,
    c02 character varying,
    c03 character varying,
    c04 character varying,
    c05 character varying,
    c06 character varying,
    c07 character varying,
    c08 character varying,
    c09 character varying,
    c10 character varying,
    c11 character varying,
    c12 character varying,
    c13 character varying
);




--create table for dataset CMS 

CREATE TABLE public.cms_2021_x (
    c01 character varying,
    c02 character varying,
    c03 character varying,
    c04 character varying,
    c05 character varying,
    c06 character varying,
    c07 character varying,
    c08 character varying,
    c09 character varying,
    c10 character varying,
    c11 character varying,
    c12 character varying,
    c13 character varying,
    c14 character varying,
    c15 character varying,
    c16 character varying,
    c17 character varying,
    aggcol double precision,
    cms_date integer,
    cms_month integer
);

CREATE TABLE public.cms_2022_x (
    c01 character varying,
    c02 character varying,
    c03 character varying,
    c04 character varying,
    c05 character varying,
    c06 character varying,
    c07 character varying,
    c08 character varying,
    c09 character varying,
    c10 character varying,
    c11 character varying,
    c12 character varying,
    c13 character varying,
    c14 character varying,
    c15 character varying,
    c16 character varying,
    c17 character varying,
    aggcol double precision,
    cms_date integer,
    cms_month integer
);
