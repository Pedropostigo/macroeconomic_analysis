-- create table for gdp in europe
CREATE TABLE IF NOT EXISTS main.gdp_eur (
    pk_geo_id TEXT
    , pk_date TEXT
    , gdp_value REAL
    , gpd_value_status TEXT
    , PRIMARY KEY (pk_geo_id, pk_date)
)