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

do $$
declare
	str varchar;
	i varchar;
begin
	for i in (select tablename from pg_tables where schemaname = 'public' 
			  								and tablename != 'table_gazprom_ru' 
			  								and tablename != 'table_gazprom_ru_final' 
			  								and tablename != 'link_list')
		loop
			str = 'DROP TABLE ' || i;	
			--raise notice '%', str;
			execute str;
		end loop;
end
$$;

