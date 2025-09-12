CREATE TABLE IF NOT EXISTS usgs.stg_earthquake_event (
	id VARCHAR
    , mag VARCHAR
    , place VARCHAR
    , time VARCHAR
    , updated VARCHAR
    , tz VARCHAR
    , url VARCHAR
    , detail VARCHAR
    , felt VARCHAR
    , cdi VARCHAR
    , mmi VARCHAR
    , alert VARCHAR
    , status VARCHAR
    , tsunami VARCHAR
    , sig VARCHAR
    , net VARCHAR
    , code VARCHAR
    , ids VARCHAR
    , sources VARCHAR
    , types VARCHAR
    , nst VARCHAR
    , dmin VARCHAR
    , rms VARCHAR
    , gap VARCHAR
    , magtype VARCHAR
    , type VARCHAR
    , title VARCHAR
    , longitude VARCHAR
    , latitude VARCHAR
    , depth VARCHAR
);


CREATE TABLE IF NOT EXISTS usgs.fact_earthquake_event (
	id SERIAL PRIMARY KEY
    , event_id VARCHAR(12) UNIQUE
    , mag DECIMAL(4,2)
    , place TEXT
    , time BIGINT
    , updated BIGINT
    , tz INT
    , url TEXT
    , detail TEXT
    , felt INT
    , cdi DECIMAL(3,1)
    , mmi DECIMAL(3,1)
    , alert VARCHAR(20)
    , status VARCHAR(30)
    , tsunami INT
    , sig INT
    , net VARCHAR(10)
    , code VARCHAR(30)
    , ids TEXT
    , sources TEXT
    , types TEXT
    , nst INT
    , dmin DECIMAL(10,5)
    , rms DECIMAL(5,2)
    , gap DECIMAL(5,2)
    , magtype VARCHAR(10)
    , type VARCHAR(50)
    , title VARCHAR(100)
    , longitude DECIMAL(9,6)
    , latitude DECIMAL(9,6)
    , depth DECIMAL(6,2)
);




SELECT * FROM usgs.stg_earthquake_event;







































