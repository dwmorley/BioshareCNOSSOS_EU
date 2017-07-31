# BioshareCNOSSOS_EU
CNOSSOS_EU Road Traffic Noise Model (modified)

For full details please see Environmental Pollution  (2015), pp. 332-341 DOI information: 10.1016/j.envpol.2015.07.031

These inputs are needed as files within the Postgres database

FILENAMES	: DEFINITION [HEADERS] <br>
- recpt	: Receptors [gid, geom(point)]<br>
- roads	: Road network, light, heavy hourly flow and speed [gid, geom(line), qh_0, ql_0 ... qh_23, ql_23, speed1, speed3]<br>
- landcover	: Land classes, 3 character Corine code [gid, geom(polygon), code_06]<br>
- buildings	: Building heights [gid, geom(polygon), height]<br>
- mettemp	: Temperature at met station [gid, geom(point), air_temp], or a constant temperature<br>
- metwind	: Wind direction proportion [gid, geom(point), ne, se, sw, nw], or a constant proportion<br>

To run the model, firstly run all lines below the commented section to create the functions in the database, then run the following lines to create the noise exposure estimates and export as a csv
```javascript
drop table if exists output;
select csharp_loop_mimic(); 
select * from output limit 100; 

copy output to 'C:/Program Files/PostgreSQL/9.2/data/output.csv' delimiter ',' csv header;
```
There is also a high-resolution version of the script (HR). This uses more accurate landcover information so does not apply a buffer around the road network. This was used to create the higher resolution comparison models in Morley et al 2015. See the paper for details.


