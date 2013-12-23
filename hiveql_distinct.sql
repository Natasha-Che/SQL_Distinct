SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
SET hive.exec.compress.output=true;
SET mapred.job.priority=NORMAL;

use natashadb;

drop table tq_eds165_uid_platform;
create table  tq_eds165_uid_platform
as
select count(distinct if(arc_x_platform_id='8ca82b03-13ea-4d33-8d11-bca9ae7e5909',arc_u_ox_id, null)) as uol_dis,
       count(distinct if(arc_x_platform_id='7264094d-d3cd-4bd6-8923-800378325043' ,arc_u_ox_id, null)) as chacha_dis,
        count(distinct if(arc_x_platform_id='a85ea5a8-9167-42df-b113-ae33aac3db8c',arc_u_ox_id, null)) as forbes_dis,
        count(distinct if(arc_x_platform_id='0238cbd0c099a74a0f401d17ffde973b8e941e1d',arc_u_ox_id, null)) as sbnation_dis,
        count(distinct if(arc_x_platform_id='6586bd37-f471-4ac9-b22e-96403fcb68df',arc_u_ox_id, null)) as zynga_dis,
        count(distinct if(arc_x_platform_id='b03b7ef0-fc65-40b1-8666-6b028b2087c0',arc_u_ox_id, null)) as blog_dis,
        count(distinct if(arc_x_platform_id='e669f271-c6cb-4ce8-adb7-44bda66a98c3',arc_u_ox_id, null)) as somo_dis,
        count(distinct if(arc_p_account=61784,arc_u_ox_id,null)) as cbs_dis,
        count(distinct if(arc_x_platform_id='8ca82b03-13ea-4d33-8d11-bca9ae7e5909' or arc_p_account=61784 or arc_x_platform_id='7264094d-d3cd-4bd6-8923-800378325043\
' or arc_x_platform_id='a85ea5a8-9167-42df-b113-ae33aac3db8c' or arc_x_platform_id='0238cbd0c099a74a0f401d17ffde973b8e941e1d' or arc_x_platform_id='6586bd37-f471-4a\
c9-b22e-96403fcb68df' or arc_x_platform_id='b03b7ef0-fc65-40b1-8666-6b028b2087c0' or arc_x_platform_id='e669f271-c6cb-4ce8-adb7-44bda66a98c3', arc_u_ox_id,null)) as\
 all_count,
        count(distinct arc_u_ox_id) as total_u_count
from default.ox3_archived_hourly where y='2013' and m='12' and d='10' and arc_x_mkt_elig and arc_e_id is not NULL and  arc_u_mkt_can_cookie=true;



