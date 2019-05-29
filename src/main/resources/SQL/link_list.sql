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
set tables_url = replace(replace(split_part(regexp_replace(rtrim(trim(seed_url), '/'), '^https?://(www.)?' , ''), '/', 1), '.' , '_'), '-', '_')

update link_list
set seed_url = trim(seed_url);
