truncate table STV2023100611__DWH.l_user_group_activity;

INSERT INTO STV2023100611__DWH.l_user_group_activity(hk_l_user_group_activity, hk_user_id,hk_group_id,load_dt,load_src)
select distinct
        hash(hk_user_id, hk_group_id) as hk_l_user_group_activity,
        hu.hk_user_id,
        hg.hk_group_id,
        now() as load_dt,
        's3' as load_src
from STV2023100611__STAGING.group_log as gl
left join STV2023100611__DWH.h_users hu on gl.user_id = hu.user_id 
left join STV2023100611__DWH.h_groups hg on gl.group_id = hg.group_id;