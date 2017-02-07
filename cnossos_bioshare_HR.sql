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
--##						 					
--##	CNOSSOS-EU ROAD TRAFFIC NOISE MODEL	 	
--##	ADAPTED FOR BIOSHARE PROJECT		 	
--##	[2012 report eur 25379 en] 		 		
--##	24hr Version				 			
--##						 					
--##	David Morley: d.morley@imperial.ac.uk	
--##	Version 1.2, 18th April 2016	 
--##						 
--#################################################

--## For full details please see Environmental Pollution  (2015), pp. 332-341 DOI information: 10.1016/j.envpol.2015.07.031
--## This is a version of the CNOSSOS-EU model as used in the above paper to make predictions for 'Model B'
--## It uses high resolution landcover data rather than corine, therefore does not buffer around the road network


drop table if exists output;
select csharp_loop_mimic(); 
select * from output;

create or replace function csharp_loop_mimic()
returns void as $$
declare 

	recpt text := 'ufpsites_shifted'; --ufpsites_shifted, facade4
	roads text := 'constant_tres_24hr_nor_bng'; 
	landcover text := 'norwich_mm_hgts';
	mettemp text := 'midas_temp_bng';
	metwind text := 'midas_winddir_bng';

	radius integer := 500; --road search buffer from receptor
	seg_dst integer := 20; --distance between each point on road

	nrow integer;
	result double precision;
	stt timestamp;
begin
	drop table if exists s;
	drop table if exists temp_rays;
	drop table if exists ray_broken;
	drop table if exists ray_details;
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
	create table ray_broken (
		gid integer,
		geom geometry,
		totlen double precision,	
		mm_id integer,
		hgt double precision,
		g double precision,
		shape_length double precision,
		near_dist double precision
	);
	create index ray_broken_indx on ray_broken using gist(geom);
	create table ray_details (
		gid integer,
		g double precision,
		gso double precision,
		gor double precision,
		dso double precision,
		dor double precision,
		hgt double precision,	
		aov segtri,
		tau double precision,
		p double precision,
		d double precision, 
		road_id integer
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
		select cnossos_3(a, recpt, roads, 
		landcover, mettemp, metwind,
		radius, seg_dst) into result; 
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


create or replace function cnossos_3(
	i integer, 
	receptors text,
	roads text,
	landcover text,
	mettemp text,
	metwind text,
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
	execute 'insert into s 
	select gid, geom from '|| receptors || '
	where gid = ' || i;

	--construct rays and road segments
	execute 'insert into temp_rays
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
	execute 'delete from temp_rays using (
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
	
	--get rays broken on landcover, g and building heights
	execute 'truncate ray_broken;
	insert into ray_broken 
	select os.id, os.geom, os.totlen, os.mm_id, os.hgt, os.g,
	st_length(os.geom) as shape_length, 
	st_distance(os.s, st_endpoint(os.geom)) as near_dist 
	from
	(select r.id, st_endpoint(r.main_ray_path) as s, get_coeffg(mm.legend) as g,
	(st_dump(st_intersection(r.main_ray_path, mm.geom))).geom as geom,
	mm.legend, mm.mm_id, mm.hgt, st_length(r.main_ray_path) as totlen
	from temp_rays as r, '|| landcover ||' as mm
	where st_intersects(r.main_ray_path, mm.geom)) as os';

	--using met data?
	execute '
	select count(relname) from pg_class 
	where relname = '''|| mettemp ||''''
	into ctemp;	
	execute '
	select count(relname) from pg_class 
	where relname = '''|| metwind ||''''
	into cwind;

	--ray_details
	execute 'insert into ray_details
	select g.gid, g.gpath, bar.gso, bar.gor, bar.dso, bar.dor, bar.hgt, aov.aov, met.tau, met.p, met.d, met.road_id
	from
	(select r.gid, sum(r.g * r.shape_length) / r.totlen as gpath
	from ray_broken as r 
	group by r.gid, r.totlen) as g 
	left join
	(with t as (
		select dst.gid, dst.dso, dst.dor, dst.hgt, r.shape_length, r.near_dist, r.g, r.totlen, r.g * r.shape_length as dg
		from ray_broken as r left join 	
		(select a.totlen - (a.near_dist + (a.shape_length / 2)) as dor, a.near_dist + (a.shape_length / 2) as dso,
		a.gid, a.totlen, a.hgt, a.geom
		from
			(select distinct on (r.gid) r.gid, r.hgt, r.geom, 
			r.mm_id, r.shape_length, r.near_dist, r.totlen
			from ray_broken as r
			order by r.gid, r.hgt desc) as a) as dst
		on r.gid = dst.gid
	)
	select source_side.gid, source_side.gso, recept_side.gor, source_side.dso, recept_side.dor, source_side.hgt
	from
		(select t.gid, sum(t.dg) / sum(t.shape_length) as gso, t.dso, t.hgt
		from t
		where t.near_dist < t.dso
		and t.hgt > 0.5
		group by t.gid, t.dso, t.hgt) as source_side
	left join
		(select t.gid, sum(t.dg) / sum(t.shape_length) as gor, t.dor
		from t
		where t.near_dist > t.dso
		and t.hgt > 0.5
		group by t.gid, t.dor) as recept_side
		on recept_side.gid = source_side.gid) as bar
	on g.gid = bar.gid
	left join
		(select r.id, get_angleofview(st_startpoint(r.main_ray_path), r.seg_start, r.seg_end, r.road) as aov
		from temp_rays as r) as aov
	on aov.id = g.gid
	left join
		(select ray.id, st_length(ray.main_ray_path) as d, get_temperature(ray.main_ray_path, '''|| mettemp ||''', '|| ctemp ||') as tau, 
		get_favourable(ray.main_ray_path, '''|| metwind ||''', '|| cwind ||') as p, ray.road_id
		from temp_rays as ray) as met
	on g.gid = met.id';

	--make calculations for each hour
	execute 'select get_noise(
	get_traffic(r.road_id, 0, '''|| roads ||'''),
	get_traffic(r.road_id, 1, '''|| roads ||'''),
	get_traffic(r.road_id, 2, '''|| roads ||'''),
	get_traffic(r.road_id, 3, '''|| roads ||'''),
	get_traffic(r.road_id, 4, '''|| roads ||'''),
	get_traffic(r.road_id, 5, '''|| roads ||'''),
	get_traffic(r.road_id, 6, '''|| roads ||'''),
	get_traffic(r.road_id, 7, '''|| roads ||'''),
	get_traffic(r.road_id, 8, '''|| roads ||'''),
	get_traffic(r.road_id, 9, '''|| roads ||'''),
	get_traffic(r.road_id, 10, '''|| roads ||'''),
	get_traffic(r.road_id, 11, '''|| roads ||'''),
	get_traffic(r.road_id, 12, '''|| roads ||'''),
	get_traffic(r.road_id, 13, '''|| roads ||'''),
	get_traffic(r.road_id, 14, '''|| roads ||'''),
	get_traffic(r.road_id, 15, '''|| roads ||'''),
	get_traffic(r.road_id, 16, '''|| roads ||'''),
	get_traffic(r.road_id, 17, '''|| roads ||'''),
	get_traffic(r.road_id, 18, '''|| roads ||'''),
	get_traffic(r.road_id, 19, '''|| roads ||'''),
	get_traffic(r.road_id, 20, '''|| roads ||'''),
	get_traffic(r.road_id, 21, '''|| roads ||'''),
	get_traffic(r.road_id, 22, '''|| roads ||'''),
	get_traffic(r.road_id, 23, '''|| roads ||'''),
	get_speed(r.road_id, '''|| roads ||'''),
	r.aov, r.hgt, r.d, r.dso, 
	r.dor, coalesce(r.g, 0), r.tau, coalesce(r.gor, 0), coalesce(r.gso, 0), r.p) as llt
	from ray_details as r';

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
	truncate ray_broken;
	truncate ray_details;
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
	dba := incoherentsum((0.0432 * (20 - coalesce(avtau, 0)) + ((4.3429 * ln(q)) + 34.581)), coalesce(dba, 0));	
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

create or replace function get_traffic(rid integer, hr integer, roads text)
returns qcat as $$
declare 
	q qcat;
	c1 double precision;
	c2 double precision;
	c3 double precision;
	c4a double precision;
	c4b double precision;
begin
	execute 'select (r.c1 / 100) * r.p' || hr || ' from '|| roads ||' as r where r.gid = ' || rid into c1;	
	execute 'select (r.c2 / 100) * r.p' || hr || ' from '|| roads ||' as r where r.gid = ' || rid into c2;
	execute 'select (r.c3 / 100) * r.p' || hr || ' from '|| roads ||' as r where r.gid = ' || rid into c3;
	execute 'select (r.c4a / 100) * r.p' || hr || ' from '|| roads ||' as r where r.gid = ' || rid into c4a;
	execute 'select (r.c4b / 100) * r.p' || hr || ' from '|| roads ||' as r where r.gid = ' || rid into c4b;
	q := (c1, c2, c3, c4a, c4b);
	return q;
end
$$ language 'plpgsql' stable;

create or replace function get_speed(rid integer, roads text)
returns vcat as $$
declare 
	v vcat;
	c1 double precision;
	c2 double precision;
	c3 double precision;
	c4a double precision;
	c4b double precision;
begin
	execute 'select r.speed1 from '|| roads ||' as r where r.gid = ' || rid into c1;
	execute 'select r.speed2 from '|| roads ||' as r where r.gid = ' || rid into c2;
	execute 'select r.speed3 from '|| roads ||' as r where r.gid = ' || rid into c3;
	execute 'select r.speed4a from '|| roads ||' as r where r.gid = ' || rid into c4a;
	execute 'select r.speed4b from '|| roads ||' as r where r.gid = ' || rid into c4b;
	v := (c1, c2, c3, c4a, c4b);
	return v;
end
$$ language 'plpgsql' stable;

--noise level calculations for one ray path 
create or replace function get_noise(
q_0 qcat,
q_1 qcat,
q_2 qcat,
q_3 qcat,
q_4 qcat,
q_5 qcat,
q_6 qcat,
q_7 qcat,
q_8 qcat,
q_9 qcat,
q_10 qcat,
q_11 qcat,
q_12 qcat,
q_13 qcat,
q_14 qcat,
q_15 qcat,
q_16 qcat,
q_17 qcat,
q_18 qcat,
q_19 qcat,
q_20 qcat,
q_21 qcat,
q_22 qcat,
q_23 qcat,
speed vcat,
aov segtri, avh double precision, d double precision, fdso double precision, fdor double precision,
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
 		dso := sqrt(fdso ^ 2 + (avh - zs) ^ 2); 
 		dor := sqrt(fdor ^ 2 + (avh - zr) ^ 2);
 		dsoi := sqrt(fdso ^ 2 + (avh + zs) ^ 2);
 		dori := sqrt(fdor ^ 2 + (avh + zr) ^ 2);	
		deltah[1] := dso + dor - d;
		deltah[2] := dsoi + dor - d;
 		deltah[3] := dso + dori - d;		
		deltaf[1] := get_curve(dso) + get_curve(dor) - get_curve(d);
		deltaf[2] := get_curve(dsoi) + get_curve(dor) - get_curve(d);
		deltaf[3] := get_curve(dso) + get_curve(dori) - get_curve(d);
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
			ch := (fm[i] * avh) / 250.0; --vi-22
			ch := least(ch, 1);
			
			gwm := get_gpath(gso, dso, zs, avh);
			--homogenous path difference vi44c1
			dif := get_dif(lamda[i], mdc, deltah, ch);	
			w := get_w(gwm, fm[i]);		
			cf := get_cf(w, dso);			
			agroundso := get_agroundh(gwm, gso, dso, kk, cf, zs, avh); 
			w := get_w(gor, fm[i]);
			cf := get_cf(w, dor);
			agroundor := get_agroundh(gor, gor, dor, kk, cf, avh, zr);			
			aboundaryh := get_adif(dif, agroundso, agroundor);			
			--favourable path difference vi44c2
			dif := get_dif(lamda[i], mdc, deltaf, ch);
			w := get_w(gso, fm[i]);
			cf := get_cf(w, dso);	
			agroundso := get_agroundf(gwm, gso, dso, kk, cf, zs, avh); 
			w := get_w(gor, fm[i]);
			cf := get_cf(w, dor);
			agroundor := get_agroundf(gor, gor, dor, kk, cf, avh, zr);
			aboundaryf := get_adif(dif, agroundso, agroundor); --vi-30, 31, 32
		end if;

		--attenuated sound levels, different conditions
		lf_oct[i] := (adiv + aatm + aboundaryf); --vi-5,6 lw - lf_oct[i]
		lh_oct[i] := (adiv + aatm + aboundaryh); --vi-7,8	

	end loop;

	--for each hour and each octave
	insert into pths select 0, get_sound(q_0, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 1, get_sound(q_1, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 2, get_sound(q_2, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 3, get_sound(q_3, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 4, get_sound(q_4, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 5, get_sound(q_5, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 6, get_sound(q_6, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 7, get_sound(q_7, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 8, get_sound(q_8, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 9, get_sound(q_9, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 10, get_sound(q_10, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 11, get_sound(q_11, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 12, get_sound(q_12, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 13, get_sound(q_13, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 14, get_sound(q_14, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 15, get_sound(q_15, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 16, get_sound(q_16, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 17, get_sound(q_17, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 18, get_sound(q_18, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 19, get_sound(q_19, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 20, get_sound(q_20, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 21, get_sound(q_21, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 22, get_sound(q_22, speed, tau, p, lf_oct, lh_oct);
	insert into pths select 23, get_sound(q_23, speed, tau, p, lf_oct, lh_oct);	
end
$$ language 'plpgsql' volatile;




--get sound power level at source before attenuation
create or replace function get_sound(
q qcat, speed vcat,
tau double precision, p double precision,
lf_oct double precision[], lh_oct double precision[])
returns llt_octave as $$
declare 
	llt double precision[8];
	tmp double precision; --air temperature correction
	k double precision[] := array[0.08, 0.04, 0.04, 0, 0]; --air temp correction 
	vm double precision; --velocity from roadtype
	q double precision; --traffic flow
	lwp double precision; --individual propulsion noise
	lwr double precision; --individual rolling noise

	--rolling a
	ar double precision[][] := array[
		[79.7, 85.7, 84.5, 90.2, 97.3, 93.9, 84.1, 74.3],
		[84, 88.7, 91.5, 96.7, 97.4, 90.9, 83.8, 80.5],
		[87.0, 91.7, 94.1, 100.7, 100.8, 94.3, 87.1, 82.5],
		[0, 0, 0, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 0, 0, 0]];
	--propulsion a
	ap double precision[][] := array[
		[94.5, 89.2, 88.0, 85.9, 84.2, 86.9, 83.3, 76.1], 
		[101, 96.5, 98.8, 96.8, 98.6, 95.2, 88.8, 82.7],
		[104.4, 100.6, 101.7, 101.0, 100.1, 95.9, 91.3, 85.3],
		[88, 87.5, 89.5, 93.7, 96.6, 98.8, 93.9, 88.7],
		[95, 97.2, 92.7, 92.9, 94.7, 93.2, 90.1, 86.5]]; 
	-- rolling b
	br double precision[][] := array[
		[30.0, 41.5, 38.9, 25.7, 32.5, 37.2, 39.0, 40.0], 
		[30, 35.8, 32.6, 23.8, 30.1, 36.2, 38.3, 40.1],
		[30.0, 33.5, 31.3, 25.4, 31.8, 37.1, 38.6, 40.6],
		[0, 0, 0, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 0, 0, 0]];
	--propulsion b
 	bp double precision[][] := array[
		[-1.3, 7.2, 7.7, 8.0, 8.0, 8.0, 8.0, 8.0], 
		[-1.9, 4.7, 6.4, 6.5, 6.5, 6.5, 6.5, 6.5],
		[0.0, 3.0, 4.6, 5.0, 5.0, 5.0, 5.0, 5.0],
		[4.2, 7.4, 9.8, 11.6, 15.7, 18.9, 20.3, 20.6],
		[3.2, 5.9, 11.9, 11.6, 11.5, 12.6, 11.1, 12]];

	vref double precision := 70.0; --velocity constant
	lwm double precision[5]; --category source noise level
	lwpr double precision; --combined rolling and propulsion
	lw double precision; --combined categories source noise level
	outllt llt_octave; --(return) 8x octaves at r before sum
	lh double precision; --sound level in homogenous
	lf double precision; --sound level in favourable
begin	
	--for each octave
	for i in 2..7 loop 
		--for vehicle categories
		--cat 1: light motor vehicles
		--cat 2: medium heavy vehicles
		--cat 3: heavy vehicles
		--cat 4a: powered-two-wheelers (<= 50cc)
		--cat 4b: powered-two-wheelers (> 50cc)
		for m in 1..5 loop		
			--air temp correction
			tmp := k[m] * (20 - tau); --iii-10

			--speed, flow for road, vehicle class
			if m = 1 then 
				vm := speed.vcat1;
				q := q.qcat1;
			elsif m = 2 then 
				vm := speed.vcat2;
				q := q.qcat2;
			elsif m = 3 then 
				vm := speed.vcat3;
				q := q.qcat3;
			elsif m = 4 then 
				vm := speed.vcat4a;
				q := q.qcat4a;
			elsif m = 5 then 
				vm := speed.vcat4b;
				q := q.qcat4b;
			end if;	

	--		raise notice 'vm: %	q: %', vm, q;

			if q is not null and q >= 0.5 then
				--individual vehicle noise
				lwr := ar[m][i] + br[m][i] * log(vm / vref) + tmp; --iii-5
				lwp := ap[m][i] + bp[m][i] * ((vm - vref) / vref); --iii-11
				lwpr := 10 * log((10 ^ (lwr / 10)) + (10 ^ (lwp / 10)));--iii-3	
				--segment flow combined noise per vehicle category
				lwm[m] := lwpr + 10 * log(q / (1000 * vm)); --iii-1
			else
				lwm[m] := 0;
			end if;
		end loop;
		
		--combined all vehicle sources at s
		lw := 10 * log( (10 ^ (lwm[1] / 10)) + (10 ^ (lwm[2] / 10)) + (10 ^ (lwm[3] / 10)) + (10 ^ (lwm[4] / 10)) + (10 ^ (lwm[5] / 10)));
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

--mastermap g coefficients
create or replace function get_coeffg(lcc varchar)
returns double precision as $$
declare 
	g double precision;
	s text;
begin
	s := lcc;
	case s
		when '0000 Foreshore', 
		'0000 Multiple surface (garde',
		 '0000 Multiple surface (garden)',
		 '0000 Natural surface',
		 '0000 Tidal water',
		 '0379 Coniferous trees',
		 '0380 Coniferous - scattered',
		 '0381 Coppice or osiers',
		 '0382 Marsh reeds or saltmarsh',
		 '0384 Nonconiferous trees',
		 '0385 Nonconiferous - scattered',
		 '0386 Orchard',
		 '0387 Heath',
		 '0390 Rough grassland',
		 '0392 Scrub',
		 '0400 Inland water'  then
			g := 1;
		else
			g := 0;
	end case;
	return g;
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

--type for traffic flow
create type qcat as (
	qcat1 double precision,
	qcat2 double precision,
	qcat3 double precision,
	qcat4a double precision,
	qcat4b double precision
);
--type for traffic low
create type vcat as (
	vcat1 double precision,
	vcat2 double precision,
	vcat3 double precision,
	vcat4a double precision,
	vcat4b double precision
);

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




