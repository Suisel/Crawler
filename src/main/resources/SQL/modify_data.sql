--create main table
create table link_list (
	link_id serial,
	seed_url varchar,
	tables_url varchar
);

select * from link_list;

insert into link_list(seed_url) values ('http://eco-gas.ru/');
insert into link_list(seed_url) values ('https://www.metan.by/');
insert into link_list(seed_url) values ('http://www.gazpromvacancy.ru/');
--insert into link_list(seed_url) values ('http://ecogas-auto.ru/');
insert into link_list(seed_url) values ('http://vbashkortostane.gazprom.ru/');
insert into link_list(seed_url) values ('http://nakubani.gazprom.ru/');
insert into link_list(seed_url) values ('http://gazprompolus.ru/');
insert into link_list(seed_url) values ('https://www.gazpromvideo.ru/');
insert into link_list(seed_url) values ('https://www.gazprom-football.com/ru/home.htm');
insert into link_list(seed_url) values ('https://www.gazprom-energy.co.uk/');
insert into link_list(seed_url) values ('http://polyanaski.ru/');

update link_list
set tables_url = replace(replace(split_part(regexp_replace(rtrim(seed_url, '/'), '^https?://(www.)?' , ''), '/', 1), '.' , '_'), '-', '_');

--create all tables from table list
do $$
declare
	str varchar;
	str_final varchar;
	i varchar;
begin
	for i in (select tables_url from link_list)
		loop
			str = 'CREATE TABLE ' || i ||
					'(
						link_id serial,
						seed varchar,
						link_path varchar,
						page_amount_www integer,
					  	page_amount integer
					)';	
			str_final = 'CREATE TABLE ' || i || '_final  
						(
							link_id serial,
							seed varchar,
							link_path varchar,
							page_amount integer
						)';	
			--raise notice '%', str_final;
			execute str;
			execute str_final;
		end loop;
end
$$;

--create tables with final values
CREATE OR REPLACE function insert_final_values()
returns varchar
as
$$ 
declare
	str varchar;
	i varchar;
begin
	for i in (select tables_url from link_list)
	loop
		str = 
			'DO
			 ''
			 DECLARE	
				v_row record;
			 BEGIN
				truncate table ' || i || '_final;
				ALTER SEQUENCE ' || i || '_final_link_id_seq RESTART WITH 1;
				FOR v_row in (select * from ' || i || ')
							LOOP
									if (v_row.page_amount_www > v_row.page_amount) then
										insert into ' || i || '_final(seed, link_path, page_amount)
										values (v_row.seed, v_row.link_path, (v_row.page_amount + v_row.page_amount_www));
									else
										insert into ' || i || '_final(seed, link_path, page_amount)
										values (v_row.seed, substring(v_row.link_path, 5), (v_row.page_amount + v_row.page_amount_www));
									end if;
							END LOOP;
			END;
			''';
		raise notice '%', str;
		execute str;
		end loop;
	return 'success';
end;																		
$$
language plpgsql;		

select insert_final_values();

--copy data to file
create or replace function copy_to_csv()
returns varchar
as
$$
declare
	i varchar;
	str varchar;
begin
	for i in (select tables_url || '_final' from link_list)
	loop
		str = 'COPY (SELECT * FROM ' || i || ')  
			   TO ''/home/elavelina/files/' || i || '.csv'' WITH DELIMITER '','' CSV';
		--raise notice '%', str;
		execute str;
	end loop;
	return 'success';
end;
$$
language plpgsql;

select copy_to_csv();