--################################################################################
--## Copyright 2014-15 David Morley
--## 
--## Licensed under the Apache License, Version 2.0 (the "License");
--## you may not use this file except in compliance with the License.
--## You may obtain a copy of the License at
--## 
--##     http://www.apache.org/licenses/LICENSE-2.0
--## 
--## Unless required by applicable law or agreed to in writing, software
--## distributed under the License is distributed on an "AS IS" BASIS,
--## WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--## See the License for the specific language governing permissions and
--## limitations under the License.
--################################################################################

--#################################################
--##						 ##
--##	CNOSSOS-EU ROAD TRAFFIC NOISE MODEL	 ##
--##	ADAPTED FOR BIOSHARE PROJECT		 ##
--##	[2012 report eur 25379 en] 		 ##
--##	24hr Version				 ##
--##						 ##
--##	David Morley: d.morley@imperial.ac.uk	 ##
--##	Version 1.3, 4th August 2015		 ##
--##						 ##
--#################################################

--## For full details please see Environmental Pollution  (2015), pp. 332-341 DOI information: 10.1016/j.envpol.2015.07.031

drop table if exists output;
select csharp_loop_mimic(); 
select * from output limit 100;

copy output to 'C:/Program Files/PostgreSQL/9.2/data/output.csv' delimiter ',' csv header;

--################################################################################################

-- FILENAMES	: DEFINITION [HEADERS]
-- recpt	: Receptors [gid, geom(point)]	
-- roads	: Road network, light, heavy hourly flow and speed [gid, geom(line), qh_0, ql_0 ... qh_23, ql_23, speed1, speed3]
-- landcover	: Land classes, 3 character Corine code [gid, geom(polygon), code_06]
-- buildings	: Building heights [gid, geom(polygon), height]
-- mettemp	: Temperature at met station [gid, geom(point), air_temp], or a constant temperature
-- metwind	: Wind direction proportion [gid, geom(point), ne, se, sw, nw], or a constant proportion

--################################################################################################

create or replace function csharp_loop_mimic()
returns void as $$
declare 
	--gb
	recpt text := 'ufpsites_shifted'; 
	roads text := 'tres_const_24hr_bng'; 
	landcover text := 'clc06_100m_v16_gb5k';
	buildings text := 'landmapheights_clcurban'; 
	mettemp text := 'midas_temp_bng';
	metwind text := 'midas_winddir_bng';


	pnt_buf double precision := 25; --buffer around receptor
	road_buf double precision := 25; --buffer around road segment
	radius integer := 500; --road search buffer from receptor
	seg_dst integer := 20; --distance between each point on road

	nrow integer;
	result double precision;
	stt timestamp;
begin
	drop table if exists s;
	drop table if exists temp_rays;
	drop table if exists noroads;
	drop table if exists ray_broken;
	drop table if exists corine_broke_rays;
	drop table if exists ray_details;
	drop table if exists rays_intersects;
	drop table if exists pths;
	drop table if exists tempout;
	drop table if exists minorflow;
	
	--output table
	create table tempout (
		gid integer,
		laeq1h_0 double precision,
		laeq1h_1 double precision,
		laeq1h_2 double precision,
		laeq1h_3 double precision,
		laeq1h_4 double precision,
		laeq1h_5 double precision,
		laeq1h_6 double precision,
		laeq1h_7 double precision,
		laeq1h_8 double precision,
		laeq1h_9 double precision,
		laeq1h_10 double precision,
		laeq1h_11 double precision,
		laeq1h_12 double precision,
		laeq1h_13 double precision,
		laeq1h_14 double precision,
		laeq1h_15 double precision,
		laeq1h_16 double precision,
		laeq1h_17 double precision,
		laeq1h_18 double precision,
		laeq1h_19 double precision,
		laeq1h_20 double precision,
		laeq1h_21 double precision,
		laeq1h_22 double precision,
		laeq1h_23 double precision,
		lday double precision,
		leve double precision,
		lnight double precision,
		laeq16 double precision,
		lden double precision,
		st_x double precision,
		st_y double precision
	);

	--temporary tables
	create table s (
		gid integer,
		geom geometry
	);
	create table temp_rays (
		gid integer,
		road geometry,
		ray_step integer,
		road_id integer,
		main_ray_path geometry,
		seg_start double precision,
		seg_end double precision,
		id serial
	);
	create index temp1_indx on temp_rays using gist(main_ray_path);
	create index temp2_indx on temp_rays using gist(road);
	create table noroads (
		geom geometry,
		id serial
	);
	create index noroads_indx on noroads using gist(geom);
	create table ray_broken (
		id integer,
		geomfrag geometry,
		r geometry,
		d double precision
	);
	create index ray_broken_indx on ray_broken using gist(geomfrag);
	create index ray_broken_indxr on ray_broken using gist(r);
	create table corine_broke_rays (
		id integer,
		geom geometry,
		clc character varying(3),
		d double precision,
		gc double precision,
		mpd double precision,
		dist_ee double precision,
		dist_se double precision
	);
	create index corine_broke_rays_indx on corine_broke_rays using gist(geom);
	create table rays_intersects (
		gid integer,
		road geometry,
		ray_step integer,
		road_id integer,
		main_ray_path geometry,
		seg_start double precision,
		seg_end double precision,
		id integer,
		o1d double precision,
		o2d double precision,
		sumhl double precision, 
		sumh double precision, 
		g double precision
	);
	create table ray_details (
		main_ray_path geometry,
		gid integer,
		id integer,
		aov segtri,
		d double precision,
		road_id integer,
		o1d double precision,
		o2d double precision,
		avh double precision,
		g double precision,
		gor double precision,
		gso double precision,
		tau double precision,
		p double precision
	);
	create table pths (
		hr integer,
		octaves llt_octave
	);
	create table minorflow (
		hour double precision,
		flow double precision
	);
	--UK 600 per day
	insert into minorflow values (0, 4.679128468);
	insert into minorflow values (1, 3.178949875);
	insert into minorflow values (2, 2.57173473);
	insert into minorflow values (3, 2.714608882);
	insert into minorflow values (4, 4.036194785);
	insert into minorflow values (5, 8.608167639);
	insert into minorflow values (6, 19.93094416);
	insert into minorflow values (7, 35.53994523);
	insert into minorflow values (8, 39.21895464);
	insert into minorflow values (9, 35.21847839);
	insert into minorflow values (10, 36.29003453);
	insert into minorflow values (11, 38.29027265);
	insert into minorflow values (12, 39.00464341);
	insert into minorflow values (13, 39.36182879);
	insert into minorflow values (14, 40.14763662);
	insert into minorflow values (15, 42.39790451);
	insert into minorflow values (16, 46.64841053);
	insert into minorflow values (17, 46.79128468);
	insert into minorflow values (18, 37.3973092);
	insert into minorflow values (19, 26.68174783);
	insert into minorflow values (20, 19.03798071);
	insert into minorflow values (21, 14.21597809);
	insert into minorflow values (22, 10.75127991);
	insert into minorflow values (23, 7.286581736);

	--do the calculations
	execute 'select count(*) from '|| recpt into nrow;	
	for a in 1..nrow loop
		select clock_timestamp() into stt;
		select cnossos_2(a, recpt, roads, 
		landcover, buildings, mettemp, metwind,
		pnt_buf, road_buf, radius, seg_dst) into result; 
		raise notice '### % of %: laeq16 = %, time: % ###', a, nrow, result, clock_timestamp() - stt;
	end loop;

	--final table
	drop table if exists output;
	execute '
	create table output as 
	select a.*,
	laeq1h_0, laeq1h_1, laeq1h_2, laeq1h_3,	laeq1h_4, laeq1h_5, laeq1h_6,
	laeq1h_7, laeq1h_8, laeq1h_9, laeq1h_10, laeq1h_11, laeq1h_12, laeq1h_13,
	laeq1h_14, laeq1h_15, laeq1h_16, laeq1h_17, laeq1h_18, laeq1h_19, laeq1h_20,
	laeq1h_21, laeq1h_22, laeq1h_23, lday, leve, lnight, laeq16, lden,
	b.st_x, b.st_y from '|| recpt ||' as a 
	left join tempout as b on a.gid = b.gid';
	truncate tempout;
end
$$ language 'plpgsql' volatile;

create or replace function cnossos_2(
	i integer, 
	receptors text,
	roads text,
	landcover text,
	buildings text,
	mettemp text,
	metwind text,
	pnt_buf double precision,
	road_buf double precision,
	radius double precision,
	seg_dst integer
) 
returns double precision as $$
declare 
	cwind integer;
	ctemp integer;
	db double precision;
	mtau double precision;
begin	
	--get receptor
	execute '
	insert into s 
	select gid, geom from ' || receptors ||
	' where gid =' || i;

	--construct rays and segments
	execute '
	insert into temp_rays
	with line as (
		--clip roads within 500m of r
		with rds as (
			select distinct rr.gid, rr.geom as recpt, t.gid as road_id,
			(st_dump(t.geom)).geom as road,	get_steprat(st_length(t.geom), '|| seg_dst ||') as step_rat
			from s as rr inner join '|| roads ||' as t 
			on st_dwithin(rr.geom, t.geom, '|| radius ||')
		)
		select * from rds
		union all
		--if no roads found in 500m of r, take the next nearest road
		select distinct rr.gid, rr.geom as recpt, t.gid as road_id,
		(st_dump(t.geom)).geom as road, get_steprat(st_length(t.geom), '|| seg_dst ||') as step_rat
		from '|| roads ||' as t, s as rr left join rds
		on rds.gid = rr.gid
		where rds.gid is null
		and t.gid = nnid(rr.geom, '|| radius ||', 2, 100, '''|| roads ||''', ''gid'', ''geom'')
	)
	select splt.gid, splt.road, splt.ray_step, splt.road_id,
	st_makeline(splt.recpt, st_line_interpolate_point(splt.road, cast(ray_step as double precision) / 100000)) as main_ray_path,		
	(cast(ray_step as double precision) / (cast(step_rat as double precision))) * cast(step_rat as double precision) as seg_start,
	(cast(ray_step as double precision) / (cast(step_rat as double precision))) * cast(step_rat as double precision) + cast(step_rat as double precision) as seg_end
	from (
		select line.gid, line.recpt, line.step_rat, line.road, line.road_id,		
		generate_series(line.step_rat / 2, 100000, line.step_rat) as ray_step
		from line
	) as splt';

	--clip rays to 500m
	execute '
	delete from temp_rays using (
		select id from
		(select r.gid, r.id, st_length(r.main_ray_path) as len
		from temp_rays as r) as lengths
		left join
				(select every.gid, every.total, outside.longer from
				(select count(r1.main_ray_path) as total, r1.gid
				from temp_rays as r1
				group by r1.gid) as every
			left join
				(select count(r1.main_ray_path) as longer, r1.gid
				from temp_rays as r1
				where st_length(r1.main_ray_path) > '|| radius ||'
				group by r1.gid) as outside
				on every.gid = outside.gid) as counts
		on lengths.gid = counts.gid
		where (len > '|| radius ||' and total != 1)
		--calculation limit stated as 2km
		or (len > 2000 and total = 1) ) as remove
	where remove.id = temp_rays.id';

	--buffer for roads used to x metres (25m)
	execute '
	insert into noroads 
	with a as (
		select st_union(st_buffer(s1.road, '|| road_buf ||')) as geom
		from 
		(select distinct road from temp_rays) as s1
	)
	select (st_dump(st_difference(l.buf, a.geom))).geom as geom
	from a, 
		(select st_union(st_buffer(rc.geom, 2000)) as buf 
		from a, s as rc) as l';

	--break rays on roads
	execute '
	insert into ray_broken
	with cut as (
		select st_makeline(st_line_interpolate_point(rp.main_ray_path, '|| pnt_buf ||' / st_length(rp.main_ray_path)), st_endpoint(rp.main_ray_path)) as cr, 
		st_length(rp.main_ray_path) as d, rp.id
		from temp_rays as rp
		where st_length(rp.main_ray_path) >= '|| pnt_buf ||'
	union all
		select rp.main_ray_path as cr, st_length(rp.main_ray_path) as d, rp.id
		from temp_rays as rp
		where st_length(rp.main_ray_path) < '|| pnt_buf ||'
	)
	select cut.id, (st_dump(st_intersection(cut.cr, c.geom))).geom as geomfrag, st_endpoint(cut.cr) as r, cut.d
	from cut, noroads as c
	where st_intersects(cut.cr, c.geom)';
	
	--intersect ray fragments with corine
	execute '
	insert into corine_broke_rays
	select c.id, c.geom, c.clc, st_length(c.geom) as d, get_coeffg(c.clc) as gc, c.d as mpd,
	st_distance(st_endpoint(c.geom), c.r) as dist_ee,
	st_distance(st_startpoint(c.geom), c.r) as dist_se 
	from
	(select a.id, (st_dump(st_intersection(a.geomfrag, lc.geom))).geom as geom, lc.code_06 as clc, a.r, a.d
	from ray_broken as a, '|| landcover ||' as lc
	where st_intersects(a.geomfrag, lc.geom)) as c';

	--intersect urban fragments with building heights
	execute '
	insert into rays_intersects
	select r.*, st_length(r.main_ray_path) - (st_length(r.main_ray_path) * h.o1) as o1d, st_length(r.main_ray_path) * h.o2 as o2d, 
	h.sumhl, h.sumh, gp.g from temp_rays as r left join
	(with lminter as (
		select (st_dump(st_intersection(a.geom, lm.geom))).geom as h, lm.height, a.id
		from (select r.geom, r.id from corine_broke_rays as r
			where r.clc = ''111'' or r.clc = ''112'' or r.clc = ''121'') as a, '|| buildings ||' as lm 
		where st_intersects(a.geom, lm.geom)
		and lm.height > 0
	)
	select max(st_line_locate_point(rp.main_ray_path, st_endpoint(hh.h))) as o1,
	min(st_line_locate_point(rp.main_ray_path, st_startpoint(hh.h))) as o2, hh.id,
	sum(hh.height * st_length(hh.h)) as sumhl,
	sum(st_length(hh.h)) as sumh
	from lminter as hh left join temp_rays as rp
	on hh.id = rp.id
	group by hh.id) as h 
	on r.id = h.id 
	left join
	(select c.id, sum(c.gc * c.d) / c.mpd as g 
	from corine_broke_rays as c
	group by c.id, c.mpd) as gp
	on gp.id = r.id';

	--using met data?
	execute '
	select count(relname) from pg_class 
	where relname = '''|| mettemp ||''''
	into ctemp;	
	execute '
	select count(relname) from pg_class 
	where relname = '''|| metwind ||''''
	into cwind;

	--make final table for noise calculation
	execute '
	insert into ray_details
	select ri.main_ray_path, ri.gid, ri.id, get_angleofview(st_startpoint(ri.main_ray_path), ri.seg_start, ri.seg_end, ri.road) as aov, st_length(ri.main_ray_path) as d,
	ri.road_id, ri.o1d, ri.o2d,	
	get_averageheight(ri.sumhl, ri.sumh) as avh,
	ri.g, difg.gor, difg.gso, get_temperature(ri.main_ray_path, '''|| mettemp ||''', '|| ctemp ||') as tau, get_favourable(ri.main_ray_path, '''|| metwind ||''', '|| cwind ||') as p
	from rays_intersects as ri left join 
	(select coalesce(o2.id, o1.id) as id, o2.gor, o1.gso from 
		(select sum(cbr.gc * cbr.d) / ri.o2d as gor, cbr.id
		from rays_intersects as ri left join corine_broke_rays as cbr 
		on ri.id = cbr.id
		where dist_se >= (cbr.mpd - ri.o2d)
		and gc != 0
		and ri.o2d > 0
		group by ri.o2d, cbr.id) as o2
	full join
		(select sum(cbr.gc * cbr.d) / ri.o1d as gso, cbr.id
		from rays_intersects as ri left join corine_broke_rays as cbr 
		on ri.id = cbr.id
		where dist_ee <= ri.o1d
		and gc != 0
		and ri.o1d > 0
		group by ri.o1d, cbr.id) as o1
	on o1.id = o2.id) as difg
	on difg.id = ri.id';

	--make calculations for each hour
	execute '
	select get_noise(
	f.qh_0, f.ql_0,
	f.qh_1, f.ql_1,
	f.qh_2, f.ql_2,
	f.qh_3, f.ql_3,
	f.qh_4, f.ql_4,
	f.qh_5, f.ql_5,
	f.qh_6, f.ql_6,
	f.qh_7, f.ql_7,
	f.qh_8, f.ql_8,
	f.qh_9, f.ql_9,
	f.qh_10, f.ql_10,
	f.qh_11, f.ql_11,
	f.qh_12, f.ql_12,
	f.qh_13, f.ql_13,
	f.qh_14, f.ql_14,
	f.qh_15, f.ql_15,
	f.qh_16, f.ql_16,
	f.qh_17, f.ql_17,
	f.qh_18, f.ql_18,
	f.qh_19, f.ql_19,
	f.qh_20, f.ql_20,
	f.qh_21, f.ql_21,
	f.qh_22, f.ql_22,
	f.qh_23, f.ql_23,
	f.speed1, f.speed3, r.aov, r.avh, r.d, r.o1d, 
	r.o2d, coalesce(r.g, 0), r.tau, coalesce(r.gor, 0), coalesce(r.gso, 0), r.p) as llt
	from ray_details as r
	left join '|| roads ||' as f
	on f.gid = r.road_id'; 

	--temperature for minor correction
	select avg(r.tau) from ray_details as r into mtau;
	--if cannot get ray average use receptor site
	if mtau is null then
		mtau := 11.0;
	end if;
	
	
	--save to results table
	insert into tempout
	with hourly as (
		select
		get_hourlydba(0, mtau) as laeq1h_0,
		get_hourlydba(1, mtau) as laeq1h_1,
		get_hourlydba(2, mtau) as laeq1h_2,
		get_hourlydba(3, mtau) as laeq1h_3,
		get_hourlydba(4, mtau) as laeq1h_4,
		get_hourlydba(5, mtau) as laeq1h_5,
		get_hourlydba(6, mtau) as laeq1h_6,
		get_hourlydba(7, mtau) as laeq1h_7,
		get_hourlydba(8, mtau) as laeq1h_8,
		get_hourlydba(9, mtau) as laeq1h_9,
		get_hourlydba(10, mtau) as laeq1h_10,
		get_hourlydba(11, mtau) as laeq1h_11,
		get_hourlydba(12, mtau) as laeq1h_12,
		get_hourlydba(13, mtau) as laeq1h_13,
		get_hourlydba(14, mtau) as laeq1h_14,
		get_hourlydba(15, mtau) as laeq1h_15,
		get_hourlydba(16, mtau) as laeq1h_16,
		get_hourlydba(17, mtau) as laeq1h_17,
		get_hourlydba(18, mtau) as laeq1h_18,
		get_hourlydba(19, mtau) as laeq1h_19,
		get_hourlydba(20, mtau) as laeq1h_20,
		get_hourlydba(21, mtau) as laeq1h_21,
		get_hourlydba(22, mtau) as laeq1h_22,
		get_hourlydba(23, mtau) as laeq1h_23
	)
	select s.gid, 
	--hourly predictions
	laeq1h_0, laeq1h_1, laeq1h_2, laeq1h_3, laeq1h_4, laeq1h_5, laeq1h_6, 
	laeq1h_7, laeq1h_8, laeq1h_9, laeq1h_10, laeq1h_11, laeq1h_12, 
	laeq1h_13, laeq1h_14, laeq1h_15, laeq1h_16, laeq1h_17, laeq1h_18, 
	laeq1h_19, laeq1h_20, laeq1h_21, laeq1h_22, laeq1h_23,

	--day
	10 * log(((10 ^ (laeq1h_7 / 10)) + (10 ^ (laeq1h_8 / 10)) + (10 ^ (laeq1h_9 / 10)) + (10 ^ (laeq1h_10 / 10)) + (10 ^ (laeq1h_11 / 10)) +
	(10 ^ (laeq1h_12 / 10)) + (10 ^ (laeq1h_13 / 10)) + (10 ^ (laeq1h_14 / 10)) + (10 ^ (laeq1h_15 / 10)) + (10 ^ (laeq1h_16 / 10)) + 
	(10 ^ (laeq1h_17 / 10)) + (10 ^ (laeq1h_18 / 10))) / 12) as lday,

	--evening
	10 * log(((10 ^ (laeq1h_19 / 10)) + (10 ^ (laeq1h_20 / 10)) + (10 ^ (laeq1h_21 / 10)) + (10 ^ (laeq1h_22 / 10))) / 4) as leve,

	--night
	10 * log(((10 ^ (laeq1h_23 / 10)) + (10 ^ (laeq1h_0 / 10)) + (10 ^ (laeq1h_1 / 10)) + (10 ^ (laeq1h_2 / 10)) +
	(10 ^ (laeq1h_3 / 10)) + (10 ^ (laeq1h_4 / 10)) + (10 ^ (laeq1h_5 / 10)) + (10 ^ (laeq1h_6 / 10))) / 8) as lnight,

	--laeq16
	10 * log(((10 ^ (laeq1h_7 / 10)) + (10 ^ (laeq1h_8 / 10)) + (10 ^ (laeq1h_9 / 10)) + (10 ^ (laeq1h_10 / 10)) + (10 ^ (laeq1h_11 / 10)) +
	(10 ^ (laeq1h_12 / 10)) + (10 ^ (laeq1h_13 / 10)) + (10 ^ (laeq1h_14 / 10)) + (10 ^ (laeq1h_15 / 10)) + (10 ^ (laeq1h_16 / 10)) + 
	(10 ^ (laeq1h_17 / 10)) + (10 ^ (laeq1h_18 / 10)) + (10 ^ (laeq1h_19 / 10)) + (10 ^ (laeq1h_20 / 10)) + (10 ^ (laeq1h_21 / 10)) +
	(10 ^ (laeq1h_22 / 10))) / 16) as laeq16,

	--lden
	10 * log(((12 * (10 ^ (((10 * log(((10 ^ (laeq1h_7 / 10)) + (10 ^ (laeq1h_8 / 10)) + (10 ^ (laeq1h_9 / 10)) + (10 ^ (laeq1h_10 / 10)) + (10 ^ (laeq1h_11 / 10)) +
	(10 ^ (laeq1h_12 / 10)) + (10 ^ (laeq1h_13 / 10)) + (10 ^ (laeq1h_14 / 10)) + (10 ^ (laeq1h_15 / 10)) + (10 ^ (laeq1h_16 / 10)) + 
	(10 ^ (laeq1h_17 / 10)) + (10 ^ (laeq1h_18 / 10))) / 12)) / 12) / 10))) + (4 * (10 ^ (((10 * log(((10 ^ (laeq1h_19 / 10)) 
	+ (10 ^ (laeq1h_20 / 10)) + (10 ^ (laeq1h_21 / 10)) + (10 ^ (laeq1h_22 / 10))) / 4)) + 5) / 10))) + 
	(8 * (10 ^ (((10 * log(((10 ^ (laeq1h_23 / 10)) + (10 ^ (laeq1h_0 / 10)) + (10 ^ (laeq1h_1 / 10)) + (10 ^ (laeq1h_2 / 10)) +
	(10 ^ (laeq1h_3 / 10)) + (10 ^ (laeq1h_4 / 10)) + (10 ^ (laeq1h_5 / 10)) + (10 ^ (laeq1h_6 / 10))) / 8)) + 10) / 10)))) / 24) as lden,

	--coordinates
	st_x(s.geom), st_y(s.geom)
	from hourly, s;

	--clear tables
	truncate s;
	truncate temp_rays;
	truncate noroads;
	truncate ray_broken;
	truncate corine_broke_rays;
	truncate ray_details;
	truncate rays_intersects;
	truncate pths;

	--report progress
	select t.laeq16 from tempout as t where gid = i into db;
	return db;
end
$$ language 'plpgsql' volatile;

--###################################################################
--### functions 
--###################################################################

--sum and apply a-weighting over octaves
create or replace function get_hourlydba(h integer, avtau double precision)
returns double precision as $$
declare 
	dba double precision;
	hq double precision;
	q double precision;
begin
	with octaves as (
		select 
		10 * log(sum((p.octaves).llt63)) as ltot63, --vi-10
		10 * log(sum((p.octaves).llt125)) as ltot125,
		10 * log(sum((p.octaves).llt250)) as ltot250,
		10 * log(sum((p.octaves).llt500)) as ltot500,
		10 * log(sum((p.octaves).llt1000)) as ltot1000,
		10 * log(sum((p.octaves).llt2000)) as ltot2000,
		10 * log(sum((p.octaves).llt4000)) as ltot4000,
		10 * log(sum((p.octaves).llt8000)) as ltot8000
		from pths as p
		where p.hr = h
	)
	select coalesce(get_dba(array[o.ltot63, o.ltot125, o.ltot250, o.ltot500, 
	o.ltot1000, o.ltot2000, o.ltot4000, o.ltot8000]), 0) 
	from octaves as o into dba;	
	--minor road correction
	select flow from minorflow where hour = h into hq;
	if hq < 1 then
		q := 1;
	else
		q := hq;
	end if;
	dba := incoherentsum( (0.0432 * (20 - coalesce(avtau, 0)) + ((4.3429 * ln(q)) + 34.581)), coalesce(dba, 0));	
	return dba;
end
$$ language 'plpgsql' stable;

--get incoherent sum of 2 noise values
create or replace function incoherentsum(a double precision, b double precision)
returns double precision as $$
declare 
	v double precision;
begin
	v := 10 * log(((10 ^ (a / 10)) + ((10 ^ (b / 10)))));
	return v;
	end;
$$ language 'plpgsql' stable;

--get path average building height
create or replace function get_averageheight(a double precision, b double precision)
returns double precision as $$
declare 
	avh double precision;
begin
	if b > 0 then
		avh := a / b;
	else
		avh := 0;
	end if;
	return avh;
end
$$ language 'plpgsql' stable;

--noise level calculations for one ray path 
create or replace function get_noise(
qh_0 double precision, ql_0 double precision,
qh_1 double precision, ql_1 double precision,
qh_2 double precision, ql_2 double precision,
qh_3 double precision, ql_3 double precision,
qh_4 double precision, ql_4 double precision,
qh_5 double precision, ql_5 double precision,
qh_6 double precision, ql_6 double precision,
qh_7 double precision, ql_7 double precision,
qh_8 double precision, ql_8 double precision,
qh_9 double precision, ql_9 double precision,
qh_10 double precision, ql_10 double precision,
qh_11 double precision, ql_11 double precision,
qh_12 double precision, ql_12 double precision,
qh_13 double precision, ql_13 double precision,
qh_14 double precision, ql_14 double precision,
qh_15 double precision, ql_15 double precision,
qh_16 double precision, ql_16 double precision,
qh_17 double precision, ql_17 double precision,
qh_18 double precision, ql_18 double precision,
qh_19 double precision, ql_19 double precision,
qh_20 double precision, ql_20 double precision,
qh_21 double precision, ql_21 double precision,
qh_22 double precision, ql_22 double precision,
qh_23 double precision, ql_23 double precision,				
speed1 double precision, speed3 double precision,
aov segtri, avh double precision, d double precision, o1d double precision, o2d double precision,
g double precision, tau double precision, gor double precision, gso double precision, p double precision)
returns void as $$ 
declare 
	zs double precision := 0.05; --equivalent source height(m) (iii-1.2)
	zr double precision := 4; --equivalent receiver height(m) (viii-1.2)
	fm double precision[] := array[63, 125, 250, 500, 1000, 2000, 4000, 8000]; --octave band centres (hz)
	lamda double precision[] := array[5.4444, 2.744, 1.372, 0.686, 0.343, 0.1715, 0.08575, 0.042875]; --wavelength (m) at band centres
	alphaatm double precision[] := array[0.105, 0.376, 1.124, 2.358, 4.079, 8.777, 26.607, 94.961]; --iso 9613-1
	kk double precision; --in vi-15
	adiv double precision; --geometrical divergence
	aatm double precision; --atmospheric absorbtion
	aboundaryh double precision; --vi-15
	aboundaryf double precision; --vi-15
	cf double precision; --in vi-16
	w double precision; --in vi-17
	llt double precision[8]; --long term sound level vi-9
	bheight boolean; --if ray intersects building height layer, and height >= 0.5m
	e double precision; --diffraction distance
	dif double precision[3]; --diffraction [s r, image s r, s image r]
	deltaf double precision[3]; --path difference under diffraction favourable [s r, image s r, s image r]
	deltah double precision[3]; --path difference under diffraction homogenous [s r, image s r, s image r]
	ch double precision; --height, diffraction
	mdc double precision; --c'' multiple diffractions correction
	dso double precision; --distance from s to top of o
	dor double precision; --distance from top of o to r	
	dsoi double precision; --distance from image s to top of o
	dori double precision; --distance from top of o to image r
	agroundso double precision; --ground attenuation s o
	agroundor  double precision; --ground attenuation o r
	gwm double precision; --g'path or gpath to use
	lf_oct double precision[8];
	lh_oct double precision[8];
begin	
	--find any type 1 diffractions
	if avh is not null and avh > 0.5 then
		--use building diffraction
		bheight := true;
		e := d - o1d - o2d; 		
 		dso := sqrt(o1d ^ 2 + (avh - zs) ^ 2); 
 		dor := sqrt(o2d ^ 2 + (avh - zr) ^ 2);
 		dsoi := sqrt(o1d ^ 2 + (avh + zs) ^ 2);
 		dori := sqrt(o2d ^ 2 + (avh + zr) ^ 2);	
		deltah[1] := dso + e + dor - d;
		deltah[2] := dsoi + e + dor - d;
 		deltah[3] := dso + e + dori - d;		
		deltaf[1] := get_curve(dso) + get_curve(e) + get_curve(dor) - get_curve(d);
		deltaf[2] := get_curve(dsoi) + get_curve(e) + get_curve(dor) - get_curve(d);
		deltaf[3] := get_curve(dso) + get_curve(e) + get_curve(dori) - get_curve(d);
	else
		--path has no urban area
		bheight := false;	
		if d <= (30 * (zs + zr)) then 
			gwm := get_gpath(g, d, zs, zr); --vi-14
		else
			gwm := g;
		end if;
	end if;

	--geometrical divergence
	adiv := -10 * log((aov.theta) / (4 * pi() * aov.rmin)); --v-3b [in draft]
	--adiv :=  20 * log(d) + 11;

	--boundary conditions
	for i in 2..7 loop 
		--calculations on elementary path
		aatm := alphaatm[i] * d / 1000; --vi-13
		kk := (2 * pi() * fm[i]) / 340; --vi-15
		
		if not bheight then
			--ground effect with no buildings
			w := get_w(gwm, fm[i]);
			cf := get_cf(w, d);
			--attenuation, homogenous conditions
			aboundaryh := get_agroundh(gwm, g, d, kk, cf, zs, zr);
			--attenuation, favourable conditions
			aboundaryf := get_agroundf(gwm, g, d, kk, cf, zs, zr);
		else
			--ground effect with building diffraction
			mdc := 1;
			if e > 0.3 then
				mdc := (1 + ((5 * lamda[i]) / e) ^ 2) / ((1.0 / 3.0) + ((5 * lamda[i]) / e) ^ 2); --vi-2			
			end if;
			ch := (fm[i] * avh) / 250.0; --vi-22
			ch := least(ch, 1);
			
			gwm := get_gpath(gso, o1d, zs, avh);
			--homogenous path difference vi44c1
			dif := get_dif(lamda[i], mdc, deltah, ch);	
			w := get_w(gwm, fm[i]);		
			cf := get_cf(w, o1d);			
			agroundso := get_agroundh(gwm, gso, o1d, kk, cf, zs, avh); 
			w := get_w(gor, fm[i]);
			cf := get_cf(w, o2d);
			agroundor := get_agroundh(gor, gor, o2d, kk, cf, avh, zr);			
			aboundaryh := get_adif(dif, agroundso, agroundor);			
			--favourable path difference vi44c2
			dif := get_dif(lamda[i], mdc, deltaf, ch);
			w := get_w(gso, fm[i]);
			cf := get_cf(w, o1d);	
			agroundso := get_agroundf(gwm, gso, o1d, kk, cf, zs, avh); 
			w := get_w(gor, fm[i]);
			cf := get_cf(w, o2d);
			agroundor := get_agroundf(gor, gor, o2d, kk, cf, avh, zr);
			aboundaryf := get_adif(dif, agroundso, agroundor); --vi-30, 31, 32
		end if;

		--attenuated sound levels, different conditions
		lf_oct[i] := (adiv + aatm + aboundaryf); --vi-5,6 lw - lf_oct[i]
		lh_oct[i] := (adiv + aatm + aboundaryh); --vi-7,8	

	end loop;

	--for each hour and each octave
	insert into pths select 0, get_sound(ql_0, qh_0, speed1, speed3, tau, p, lf_oct, lh_oct);
 	insert into pths select 1, get_sound(ql_1, qh_1, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 2, get_sound(ql_2, qh_2, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 3, get_sound(ql_3, qh_3, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 4, get_sound(ql_4, qh_4, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 5, get_sound(ql_5, qh_5, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 6, get_sound(ql_6, qh_6, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 7, get_sound(ql_7, qh_7, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 8, get_sound(ql_8, qh_8, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 9, get_sound(ql_9, qh_9, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 10, get_sound(ql_10, qh_10, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 11, get_sound(ql_11, qh_11, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 12, get_sound(ql_12, qh_12, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 13, get_sound(ql_13, qh_13, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 14, get_sound(ql_14, qh_14, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 15, get_sound(ql_15, qh_15, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 16, get_sound(ql_16, qh_16, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 17, get_sound(ql_17, qh_17, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 18, get_sound(ql_18, qh_18, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 19, get_sound(ql_19, qh_19, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 20, get_sound(ql_20, qh_20, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 21, get_sound(ql_21, qh_21, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 22, get_sound(ql_22, qh_22, speed1, speed3, tau, p, lf_oct, lh_oct);
	insert into pths select 23, get_sound(ql_23, qh_23, speed1, speed3, tau, p, lf_oct, lh_oct);
end
$$ language 'plpgsql' volatile;

--get sound power level at source before attenuation
create or replace function get_sound(ql double precision, qh double precision, 
speed1 double precision, speed3 double precision, tau double precision, p double precision,
lf_oct double precision[], lh_oct double precision[])
returns llt_octave as $$
declare 
	llt double precision[8];
	tmp double precision; --air temperature correction
	k double precision[] := array[0.08, 0.04]; --air temp correction [light, heavy]
	vm double precision; --velocity from roadtype
	q double precision; --traffic flow
	lwp double precision; --individual propulsion noise
	lwr double precision; --individual rolling noise
	--final heavy class 3
	ar double precision[][] := array[[79.7, 85.7, 84.5, 90.2, 97.3, 93.9, 84.1, 74.3], [87.0, 91.7, 94.1, 100.7, 100.8, 94.3, 87.1, 82.5]];--rolling a
	ap double precision[][] := array[[94.5, 89.2, 88.0, 85.9, 84.2, 86.9, 83.3, 76.1], [104.4, 100.6, 101.7, 101.0, 100.1, 95.9, 91.3, 85.3]];--propulsion a
	br double precision[][] := array[[30.0, 41.5, 38.9, 25.7, 32.5, 37.2, 39.0, 40.0], [30.0, 33.5, 31.3, 25.4, 31.8, 37.1, 38.6, 40.6]];-- rolling b
 	bp double precision[][] := array[[-1.3, 7.2, 7.7, 8.0, 8.0, 8.0, 8.0, 8.0], [0.0, 3.0, 4.6, 5.0, 5.0, 5.0, 5.0, 5.0]]; --propulsion b
	vref double precision := 70.0; --velocity constant
	lwm double precision[2]; --category source noise level
	lwpr double precision; --combined rolling and propulsion
	lw double precision; --combined categories source noise level
	outllt llt_octave; --(return) 8x octaves at r before sum
	lh double precision; --sound level in homogenous
	lf double precision; --sound level in favourable
begin
	--if no traffic or negative traffic
	if ql <= 0 and qh <= 0 then
		return null;
	end if;
	ql := greatest(ql, 0);
	qh := greatest(qh, 0);
	
	--for each octave
	for i in 2..7 loop 
		--for light then heavy categories
		for m in 1..2 loop
			--air temp correction
			tmp := k[m] * (20 - tau); --iii-10
			--speed, flow for road, vehicle class
			if m = 1 then 
				vm := speed1;
				q := ql;
			else 
				vm := speed3;
				q := qh;
			end if;
			--individual vehicle noise
			lwr := ar[m][i] + br[m][i] * log(vm / vref) + tmp; --iii-5
			lwp := ap[m][i] + bp[m][i] * ((vm - vref) / vref); --iii-11
			lwpr := 10 * log((10 ^ (lwr / 10)) + (10 ^ (lwp / 10)));--iii-3	
			--segment flow combined noise per category
			if q > 0 then
				lwm[m] := lwpr + 10 * log(q / (1000 * vm)); --iii-1
			else
				lwm[m] := 0;
			end if;
		end loop;
		
		--combined light and heavy sources at s
		lw := 10 * log((10 ^ (lwm[1] / 10)) + (10 ^ (lwm[2] / 10)));
		lf := lw - greatest(lf_oct[i], 0);
		lh := lw - greatest(lh_oct[i], 0);
			
		--integrated long term sound level for path sr per octave
		llt[i] := 10 * log((p * (10 ^ (lf / 10))) + ((1 - p) * (10 ^ (lh / 10)))); --vi-9
	end loop;
	
	outllt := (get_lglt(llt[1]), get_lglt(llt[2]), get_lglt(llt[3]), get_lglt(llt[4]), 
	get_lglt(llt[5]), get_lglt(llt[6]), get_lglt(llt[7]), get_lglt(llt[8]));
	return outllt;
end
$$ language 'plpgsql' stable;

--get curved ray path, vi-25,26
create or replace function get_curve(d double precision)
returns double precision as $$
declare 
	hat double precision;
	gamma double precision;
begin
	gamma := 8 * d;
	gamma := greatest(1000, gamma);  --vi-24
	hat := (2 * gamma * asin(d / (2 * gamma)));
	return hat;
end
$$ language 'plpgsql' stable;

--get adif, vi-30 
create or replace function get_adif(dif double precision[3], agroundso double precision, agroundor double precision)
returns double precision as $$
declare 
	adif double precision;
begin
	adif := dif[1] + --vi-21
	(-20 * log(1 + (((10 ^ (-agroundso / 20)) - 1) * (10 ^ (-(dif[2] - dif[1]) / 20))))) + --vi-31
	(-20 * log(1 + (((10 ^ (-agroundor / 20)) - 1) * (10 ^ (-(dif[3] - dif[1]) / 20)))));  --vi-32
	return adif;
	exception when invalid_argument_for_logarithm then
		return dif[1];
	end;
$$ language 'plpgsql' stable;

--get pure diffraction, delta dif, vi-21
create or replace function get_dif(lamda double precision, mdc double precision, 
delta double precision[3], ch double precision)
returns double precision[3] as $$
declare 
	dif double precision[3];
begin
	for i in 1..3 loop
		if ((40 / lamda) * mdc * delta[i]) >= -2 then
			dif[i] := 10 * ch * log(3 + ((40 / lamda)) * mdc * delta[i]); 
			if dif[i] < 0 then
				dif[i] := 0;
			end if;
			if i = 1 then
				dif[i] := least(dif[i], 25);
			end if;
		else
			dif[i] := 0; 
		end if;
	end loop;
	return dif;
end
$$ language 'plpgsql' stable;

--get cf for vi-16,17
create or replace function get_cf(w double precision, d double precision)
returns double precision as $$
declare 
	cf double precision;
begin
	cf := d * ((1 + (3 * w * d * exp(-(sqrt(w * d))))) / (1 + (w * d))); 
	return cf;
end
$$ language 'plpgsql' stable;

--get aground favourable
create or replace function get_agroundf(g double precision, gpath double precision, d double precision, 
kk double precision, cf double precision, zs double precision, zr double precision)
returns double precision as $$
declare 
	agroundf double precision;
	x double precision;
	zsc double precision;
	zrc double precision;
begin
	zsc := zs + (0.0002 * ((((zs / (zs + zr)) ^ 2) * ((d ^ 2) / 2)))) + (0.006 * (d / (zs + zr))); --vi-19
	zrc := zr + (0.0002 * ((((zr / (zs + zr)) ^ 2) * ((d ^ 2) / 2)))) + (0.006 * (d / (zs + zr)));
	if d <= 30 * (zsc + zrc) then --vi-20
		x := -3 * (1 - g);
	else
		x := (-3 * (1 - g)) * (1 + (2 * (1 - ((30 * (zsc + zrc)) / d))));
	end if;
	if gpath = 0 then
		agroundf := x;
	else
		agroundf := -10 * log((4 * (kk ^ 2) / (d ^ 2)) * ((zsc ^ 2) - 
		(sqrt(((2 * cf) / kk)) * zsc) + (cf / kk)) * 
		((zrc ^ 2) - (sqrt(((2 * cf) / kk)) * zrc) + (cf / kk))); --vi-15
		if agroundf < x then agroundf := x;
		end if;	
	end if;
	return agroundf;
end
$$ language 'plpgsql' stable;

--get aground homogenous
create or replace function get_agroundh(g double precision, gpath double precision, d double precision, 
kk double precision, cf double precision, zs double precision, zr double precision)
returns double precision as $$
declare 
	agroundh double precision;
	x double precision;
begin
	if gpath = 0 then
		agroundh := -3;
	else
		agroundh := -10 * log((4 * (kk ^ 2) / (d ^ 2)) * ((zs ^ 2) - 
		(sqrt(((2 * cf) / kk)) * zs) + (cf / kk)) * 
		((zr ^ 2) - (sqrt(((2 * cf) / kk)) * zr) + (cf / kk))); --vi-15
		x := -3 * (1 - g);
		if agroundh < x then agroundh := x;
		end if;  
	end if;
	return agroundh;
end
$$ language 'plpgsql' stable;

--get w for vi-16,17
create or replace function get_w(g double precision, fm double precision)
returns double precision as $$
declare 
	w double precision;
begin
	w := 0.0185 * (((fm ^ 2.5) * (g ^ 2.6)) / (((fm ^ 1.5) * (g ^ 2.6)) + 
	(1300 * ((fm ^ 0.75) * (g ^ 1.3))) + 1160000)); --vi-17
	return w;
end
$$ language 'plpgsql' stable;

--log transform sound level
create or replace function get_lglt(lt double precision)
returns double precision as $$
declare 
	t double precision;
begin
	t := 10 ^ (lt / 10);
	return t;
end
$$ language 'plpgsql' stable;

--integrate octave band totals to final dba level
create or replace function get_dba(lt double precision[8])
returns double precision as $$
declare 
	awc double precision[] := array[-26.22302364022129, -16.189851062139507, -8.675022287681816, 
	-3.2479917598088837, 0, 1.2016993444284976, 0.9642291552357972, -1.144507424157968]; --iec 61672:2003
	t double precision;
begin
	t := 0;
	for i in 2..7 loop
		t := t + (10 ^ ((lt[i] + awc[i]) / 10)); 
	end loop;
	if t = 0 then
		return 0;
	end if;
	t := 10 * log(t);
	if t < 0 then
		t := 0;
	end if;
	return t; --vi-11 (is the final result)
end
$$ language 'plpgsql' stable;

--get step ratio
create or replace function get_steprat(lgth double precision, x integer)
returns integer as $$
declare 
	step integer;
begin
	if lgth < x then
		--take only midpoint of road sections < x m long
		step := 100000;
	else
		--x metre intervals
		step := cast(trunc(100000 / (lgth / x)) as integer);
	end if;
	return step;
end
$$ language 'plpgsql' stable;

--get air temperature at source
create or replace function get_temperature(r geometry, stations text, b integer)
returns double precision as $$
declare 
	tau double precision;
	nn integer;
begin
	if b = 0 then
		return cast(stations as double precision);
	else
		nn := nnid(r, 15000, 2, 100, stations, 'src_id', 'geom');
		execute '
		select w.air_temp from '|| stations ||' as w
		where w.src_id = ' || nn into tau;
		return tau;
	end if;
end
$$ language 'plpgsql' stable;


--get probability of favourable conditions for path
create or replace function get_favourable(rp geometry, stations text, b integer)
returns double precision as $$
declare 
	p double precision;
	az double precision;
	d varchar;
	s geometry;
	nn integer;
begin	
	if b = 0 then
		return cast(stations as double precision);
	else
		s := st_endpoint(rp);
		az := degrees(st_azimuth(st_startpoint(rp), s)); 
		if az >= 0 and az < 90 then 		
			d := 'ne';
		elsif az >= 90 and az < 180 then 
			d := 'se';
		elsif az >= 180 and az < 270 then 
			d := 'sw';
		else 					
			d := 'nw';
		end if;
		nn := nnid(s, 15000, 2, 100, stations, 'src_id', 'geom');
		execute 'select w.' || d || ' from '|| stations ||' as w where w.src_id = ' || nn
		into p;
		return p;
	end if;
end
$$ language 'plpgsql' stable;

--get multiplier for g based on corine class, table vi-1
create or replace function get_coeffg(lcc varchar)
returns double precision as $$
declare 
	g double precision;
	s text;
begin
	s := substring(lcc from 1 for 2);
	case s
		when '11', '12', '13',
		'51', '52' then
		  g := 0.0; --hard things
		when '33' then
		  g := 0.3;
		when '14' then 
		  g := 0.7;
		when '21', '22', '23', '24',
		'31', '32', '41', '42' then
		  g := 1.0; --soft things
		else
		  g := 0.0;
	end case;
	return g;
end
$$ language 'plpgsql' stable;

--get corrected g path, vi-14
create or replace function get_gpath(g double precision, d double precision, 
zs double precision, zr double precision)
returns double precision as $$
declare 
	gpath double precision;	
	gs double precision := 0; --ground effect at source (road)
begin
	gpath := (g * (d / (30 * (zs + zr)))) + (gs * (1 - (d / (30 * (zs + zr)))));
	return gpath;
end
$$ language 'plpgsql' stable;

--get angle of segment view from receptor, radians
create or replace function get_angleofview(r geometry, stt double precision, 
	stp double precision, road geometry)
returns segtri as $$
declare 
	triangle segtri;
	anglea double precision; 
	angleb double precision;
	anglec double precision;
	pdist double precision;
	radist float;
	rbdist float;
	abdist float;
	a geometry;
	b geometry;
begin
	stp := least(stp, 100000); --snap to end of road
	a := st_line_interpolate_point(road, stt / 100000);
	b := st_line_interpolate_point(road, stp / 100000);
	abdist := st_distance(a, b);
	radist := st_distance(a, r);
	rbdist := st_distance(b, r);
	--from crtn noise model
	if abdist = 0 or radist = 0 or rbdist = 0 then
		anglea := 0; 
		anglec := 0; 
	else
		anglea := (((rbdist ^ 2) - (radist ^ 2) - (abdist ^ 2)) / (-2 * radist * abdist)); 
		anglec := (((abdist ^ 2) - (radist ^ 2) - (rbdist ^ 2)) / (-2 * radist * rbdist)); 
	end if;
	if anglea > 1 or anglea < -1 then
		anglea := 0;
	else
		anglea := acos(anglea);
	end if;
	if anglec > 1 or anglec < -1 then
		anglec := 0;
	else
		anglec := acos(anglec);
	end if;
	angleb := pi() - anglea - anglec;
	if anglea = pi() / 2 then
		pdist := radist;
	elsif angleb = pi() / 2 then
		pdist := rbdist;
	elsif anglea = pi() and angleb = 0 then
		pdist := 3.5;
		anglec := radians(1);
	elsif anglea = 0 and angleb = pi() then
		pdist := 3.5;
		anglec := radians(1);
	elsif anglea = 0 and angleb = 0 then
		pdist := 3.5;
		anglec := 2 * pi();
	elsif anglea > pi() / 2 and anglea < pi() then
		pdist := sin(pi() - anglea) * radist;
	elsif angleb > pi() / 2 and angleb < pi() then
		pdist := sin(pi() - angleb) * rbdist;
	elsif anglea < pi() / 2 and angleb < pi() / 2 then
		pdist := sin(anglea) * radist;
	end if;
	if anglec = 0 then
		anglec = radians(0.00001);
	end if;
	if pdist = 0 then
		pdist = 3.5;
	end if;
	triangle := (anglec, pdist);
	return triangle; 
end
$$ language 'plpgsql' stable;


--http://gis.stackexchange.com/questions/14456/finding-the-closest-geometry-in-postgis
create or replace function  nnid(nearto geometry, initialdistance real, distancemultiplier real, 
maxpower integer, nearthings text, nearthingsidfield text, nearthingsgeometryfield  text)
returns integer as $$
declare 
  sql text;
  result integer;
begin
  sql := ' select ' || quote_ident(nearthingsidfield) 
      || ' from '   || quote_ident(nearthings)
      || ' where st_dwithin($1, ' 
      ||   quote_ident(nearthingsgeometryfield) || ', $2 * ($3 ^ $4))'
      || ' order by st_distance($1, ' || quote_ident(nearthingsgeometryfield) || ')'
      || ' limit 1';
  for i in 0..maxpower loop
     execute sql into result using nearto             -- $1
                                , initialdistance     -- $2
                                , distancemultiplier  -- $3
                                , i;                  -- $4
     if result is not null then return result; end if;
  end loop;
  return null;
end
$$ language 'plpgsql' stable;

--return type for single path noise
create type llt_octave as (
	llt63 double precision,
	llt125 double precision,
	llt250 double precision,
	llt500 double precision,
	llt1000 double precision,
	llt2000 double precision,
	llt4000 double precision,
	llt8000 double precision
);

--return type for segment angle and distance
create type segtri as (
	theta double precision, --angle of view
	rmin double precision --perpendicular dist
);