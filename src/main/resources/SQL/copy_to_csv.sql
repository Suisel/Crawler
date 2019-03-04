SELECT link_id, seed_url, tables_url
	FROM public.link_list;

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

COPY (SELECT * FROM ETL_1)  TO '/home/elavelina/files//test.csv' WITH DELIMITER ',' CSV;