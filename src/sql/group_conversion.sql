with 
	user_group_log as (
	    select hg.hk_group_id,
			count(distinct luga.hk_user_id) as cnt_added_users
			from STV2023100611__DWH.h_groups hg 
			join STV2023100611__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
			JOIN STV2023100611__DWH.s_auth_history sah on luga.hk_l_user_group_activity  = sah.hk_l_user_group_activity
		where sah.event = 'add'
		group by hg.hk_group_id,hg.registration_dt
		order by hg.registration_dt asc
		limit 10), 
	user_group_messages as (
		select hg.hk_group_id,
			count(distinct hu.hk_user_id)as cnt_message
			from STV2023100611__DWH.h_groups hg 
			join STV2023100611__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
			join STV2023100611__DWH.h_users hu on luga.hk_user_id = hu.hk_user_id
			join STV2023100611__DWH.l_user_message lum on hu.hk_user_id = lum.hk_user_id
		group by hg.hk_group_id          
)select  
	ugl.hk_group_id,
	ugl.cnt_added_users as cnt_added_users,
	ugm.cnt_message as cnt_users_in_group_with_messages,
	(ugm.cnt_message / ugl.cnt_added_users) as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by group_conversion desc;