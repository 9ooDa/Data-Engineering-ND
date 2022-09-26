# insert statements
insert_fact="""
INSERT INTO fact_immigration (cic_id, year, month, dep_city, arrival_date, dep_date, \
                            travel_code, visa, country) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

insert_dim_imm_per="""
INSERT INTO dim_immigration_personal (cic_id, citizen_country, resident_country, birthyear, \
                                    gender, ins_number) \
VALUES (%s,%s,%s,%s,%s,%s)
"""

insert_dim_imm_air="""
INSERT INTO dim_immigration_air (cic_id, airline, admin_number, flight_number, visa, visa_type) \
VALUES (%s,%s,%s,%s,%s,%s)
"""

insert_dim_demo_info="""
INSERT INTO dim_demo_info (city, state, m_population, f_population, total_population, \
                        num_of_veterans, foreign_born, state_code, race) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

insert_dim_demo_stat="""
INSERT INTO dim_demo_stat (city, state, median_age, avg_household_size, state_code) \
VALUES (%s,%s,%s,%s,%s)
"""

insert_dim_temp="""
INSERT INTO dim_temp (dt, avg_temp, avg_temp_uncertainty, city, country, latitude, longitude, \
                    month, year) \
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# auxiliary tables
insert_cntry_code="""
INSERT INTO aux_country_code (country_code, country_name) \
VALUES (%s, %s)
"""

insert_port_code="""
INSERT INTO aux_port_code (port_code, port_loc_city, port_loc_state) \
VALUES (%s, %s, %s)
"""

insert_mode="""
INSERT INTO aux_mode (code, mode) \
VALUES (%s, %s)
"""

insert_state_code="""
INSERT INTO aux_state_code (state_code, state) \
VALUES (%s, %s)
"""

insert_visa="""
INSERT INTO aux_visa (code, visa) \
VALUES (%s, %s)
"""