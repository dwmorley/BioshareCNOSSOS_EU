# BioshareCNOSSOS_EU
CNOSSOS_EU Road Traffic Noise Model (modified)

For full details please see Environmental Pollution  (2015), pp. 332-341 DOI information: 10.1016/j.envpol.2015.07.031

These inputs are needed as files within the Postgres database
-- FILENAMES	: DEFINITION [HEADERS]

-- recpt	: Receptors [gid, geom(point)]

-- roads	: Road network, light, heavy hourly flow and speed [gid, geom(line), qh_0, ql_0 ... qh_23, ql_23, speed1, speed3]

-- landcover	: Land classes, 3 character Corine code [gid, geom(polygon), code_06]

-- buildings	: Building heights [gid, geom(polygon), height]

-- mettemp	: Temperature at met station [gid, geom(point), air_temp], or a constant temperature

-- metwind	: Wind direction proportion [gid, geom(point), ne, se, sw, nw], or a constant proportion


To run the model, firstly run all lines below the commented section to create the functions in the database, then run the following lines to create the noise exposure estimates and export as a csv

drop table if exists output;
select csharp_loop_mimic(); 
select * from output limit 100; 
copy output to 'C:/Program Files/PostgreSQL/9.2/data/output.csv' delimiter ',' csv header;


