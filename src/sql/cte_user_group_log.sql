with user_group_log as (
    select hg.hk_group_id,
		count(distinct luga.hk_user_id) as cnt_added_users
		from STV2023100611__DWH.h_groups hg 
		join STV2023100611__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
		JOIN STV2023100611__DWH.s_auth_history sah on luga.hk_l_user_group_activity  = sah.hk_l_user_group_activity
	where sah.event = 'add'
	group by hg.hk_group_id,hg.registration_dt
	order by hg.registration_dt asc
	limit 10)
select hk_group_id,
       cnt_added_users
from user_group_log
order by cnt_added_users
limit 10;