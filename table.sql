CREATE TABLE IF NOT EXISTS china_area_info(
	id int8 NOT NULL,
	number varchar(20) NOT NULL,
	name text NOT NULL,
	full_name text NOT NULL,
	type int4 NULL,
	level int4 NOT NULL,
	year int4 NOT NULL,
	parents_id _int8 NOT NULL,
	release_date date NOT NULL,
	create_time timestamp NOT NULL DEFAULT now(),
    CONSTRAINT china_area_info_pk PRIMARY KEY (id,year)
);