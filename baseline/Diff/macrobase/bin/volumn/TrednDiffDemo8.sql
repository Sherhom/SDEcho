IMPORT FROM CSV FILE '/home/zx/csv/webret2000_8.csv' INTO
    store_returns(
        d_date char(100),
        d_date_seq char(100),
        timeline double,
        d_week_seq char(100),
        year double,
        i_brand char(100),
        i_class char(100),
        category char(100),
        manufact char(100),
        i_color char(100),
        street_name char(100),
        street_type char(100),
        city char(100),
        county char(100),
        state char(100),
        ca_location_type char(100),
        customerflag char(100),
        c_birth_year char(100),
        c_first_sales_date_sk char(100),
       reason char(100));

SELECT * FROM
             DIFF
                 (SELECT * FROM store_returns WHERE year > 2000.5) outliers,
             (SELECT * FROM store_returns WHERE year < 2000.5) inliers
    ON i_brand,i_class,category,manufact,i_color,street_name,county WITH MIN TRENDDIFF -385.96;