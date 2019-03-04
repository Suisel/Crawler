SELECT link_id, seed, link_path, page_amount_www, page_amount
	FROM public.table_gazprom_ru;
	
	
create table test as 
select * from table_gazprom_ru where link_path = '';

SELECT link_id, seed, link_path, page_amount_www, page_amount
	FROM public.test;

drop function insert_final_values();

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




DO
$$
DECLARE	
	v_row record;
BEGIN
				
	truncate table eco_gas_ru_final;
	ALTER SEQUENCE eco_gas_ru_final_link_id_seq RESTART WITH 1;
	FOR v_row in (select * from eco_gas_ru)
				LOOP
						if (v_row.page_amount_www > v_row.page_amount) then
							insert into eco_gas_ru_final(seed, link_path, page_amount)
							values (v_row.seed, v_row.link_path, (v_row.page_amount + v_row.page_amount_www));
						else
							insert into eco_gas_ru_final(seed, link_path, page_amount)
							values (v_row.seed, substring(v_row.link_path, 5), (v_row.page_amount + v_row.page_amount_www));
						end if;
				END LOOP;
END;
$$