IMPORT FROM CSV FILE '/home/zx/workspace/macrobase/sql/data/nat_1.csv' INTO
    sdecho(
        tableid double,
        timeline double,
        agg double,
        c01 char(100),
        c02 char(100),
        c03 char(100),
        c04 char(100),
        c05 char(100),
        c06 char(100),
        c07 char(100),
        c08 char(100),
        c09 char(100),
        c10 char(100),
        c11 char(100),
        c12 char(100)
        );

SELECT * FROM
             DIFF
                 (SELECT * FROM sdecho WHERE tableid > 1.5) outliers,
             (SELECT * FROM sdecho WHERE tableid < 1.5) inliers
    ON c01,c02,c03,c04,c05,c06 WITH MIN TRENDDIFF -100000000.0 ;
