with user_group_messages as (
select hg.hk_group_id,
		count(distinct hu.hk_user_id)as cnt_message
		from STV2023100611__DWH.h_groups hg 
		join STV2023100611__DWH.l_user_group_activity luga on hg.hk_group_id = luga.hk_group_id
		join STV2023100611__DWH.h_users hu on luga.hk_user_id = hu.hk_user_id
		join STV2023100611__DWH.l_user_message lum on hu.hk_user_id = lum.hk_user_id
	group by hg.hk_group_id          
)
select hk_group_id,
       cnt_message as cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10; 