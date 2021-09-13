DROP TABLE IF EXISTS fact_immigration;
DROP TABLE IF EXISTS dim_immigration_personal;
DROP TABLE IF EXISTS dim_immigration_air;
DROP TABLE IF EXISTS dim_demo_info;
DROP TABLE IF EXISTS dim_demo_stat;
DROP TABLE IF EXISTS dim_temp;


CREATE TABLE IF NOT EXISTS public.fact_immigration (
	cic_id float PRIMARY KEY,
	year float,
	month float,
	dep_city varchar(256),
	arrival_date datetime,
    dep_date datetime,
    travel_code float,
    visa float,
    country varchar
);


CREATE TABLE IF NOT EXISTS public.dim_immigration_personal (
    cic_id float PRIMARY KEY,
    citizen_country float,
    resident_country float,
    birthyear float,
    gender varchar,
    ins_number float    
);


CREATE TABLE IF NOT EXISTS public.dim_immigration_air (
    cic_id float PRIMARY KEY,
    airline varchar,
    admin_number float,
    flight_number int,
    visa float,
    visa_type varchar
);


CREATE TABLE IF NOT EXISTS public.dim_demo_info (
    city varchar PRIMARY KEY,
    state varchar,
    m_population float,
    f_population float,
    total_population int,
    num_of_veterans float,
    foreign_born float,
    state_code varchar,
    race varchar
);


CREATE TABLE IF NOT EXISTS public.dim_demo_stat (
    city varchar PRIMARY KEY,
    state varchar,
    median_age float,
    avg_household_size float,
    state_code varchar
);


CREATE TABLE IF NOT EXISTS public.dim_temp (
    dt datetime PRIMARY KEY,
    avg_temp float,
    avg_temp_uncertainty float,
    city varchar,
    country varchar,
    latitude varchar,
    longitude varchar,
    month int,
    year int
);