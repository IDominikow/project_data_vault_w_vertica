drop table if exists STV2023100611__STAGING.group_log;

create table if not exists STV2023100611__STAGING.group_log(
	id IDENTITY primary key not null,
	group_id integer,
	user_id integer,
	user_id_from integer,
	event varchar(20),
	"datetime" timestamp(0)
	)order by group_id, user_id
	PARTITION BY "datetime"::date
	GROUP BY calendar_hierarchy_day("datetime"::date, 3, 2);