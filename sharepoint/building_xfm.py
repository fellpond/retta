# Databricks notebook source
building_df = spark.table('retta.dim_building_stg')
meter_df = spark.table('retta.dim_meter_stg')
meter_reln_df = spark.table('retta.dim_meter_relation_stg')
#display(building_df)

# COMMAND ----------

# MAGIC %sql with cte_join as (
# MAGIC   select
# MAGIC     mtr.meter_ID,
# MAGIC     building_ID,
# MAGIC     mtr.ean_num,
# MAGIC     energy.mnthly_strt_dt,
# MAGIC     energy.mnthly_end_dt,
# MAGIC     energy.reading_dt,
# MAGIC     energy.mnthly_units as main_units,
# MAGIC     enrgy_a.mnthly_units as sub_units,
# MAGIC     sub_meter_id
# MAGIC   from
# MAGIC     retta.dim_meter_stg mtr
# MAGIC     join retta.energy_monthly_tgt_stg energy on mtr.ean_num = energy.ean
# MAGIC     join retta.dim_meter_relation_stg mtr_reln on mtr.ean_num = mtr_reln.meter_id
# MAGIC     left join retta.energy_monthly_tgt_stg enrgy_a on mtr_reln.sub_meter_id = enrgy_a.ean and energy.mnthly_end_dt = enrgy_a.mnthly_end_dt
# MAGIC )
# MAGIC  select building_ID, mnthly_strt_dt, mnthly_end_dt, concat(year(mnthly_end_dt), right(concat('0',month(mnthly_end_dt)),2)) as yrmnth, (sum(distinct(main_units)) - sum((case when cte.sub_units is null then 0 else cte.sub_units end))) as net_units from cte_join cte group by building_ID, mnthly_strt_dt, mnthly_end_dt;

# COMMAND ----------

_sqldf.createOrReplaceTempView('b_comp')
display(spark.sql('select * from b_comp where building_id = 4 order by mnthly_end_dt'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC     mtr.meter_ID,
# MAGIC     building_ID,
# MAGIC     mtr.ean_num,
# MAGIC     energy.mnthly_strt_dt,
# MAGIC     energy.mnthly_end_dt,
# MAGIC     energy.reading_dt,
# MAGIC     energy.mnthly_units as main_units,
# MAGIC    
# MAGIC     sub_meter_id
# MAGIC   from
# MAGIC     retta.dim_meter_stg mtr
# MAGIC     join retta.energy_monthly_tgt_stg energy on mtr.ean_num = energy.ean
# MAGIC     join retta.dim_meter_relation_stg mtr_reln on mtr.ean_num = mtr_reln.meter_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.energy_monthly_tgt_stg where ean='20003317'

# COMMAND ----------

 cte_out as (
 select building_ID, mnthly_strt_dt, mnthly_end_dt, concat(year(mnthly_end_dt), right(concat('0',month(mnthly_end_dt)),2)) as yrmnth, (sum(main_units) - sum((case when cte.sub_units is null then 0 else cte.sub_units end))) as net_units from cte_join cte group by building_ID, mnthly_strt_dt, mnthly_end_dt)
 select * from cte_out where building_Id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select EAN, mnthly_end_dt, mnthly_units  from retta.energy_monthly_tgt_stg where ean in ('20003536',
# MAGIC '20004419',
# MAGIC '80040887624600475931',
# MAGIC '80040887624600477034'
# MAGIC ) ORDER BY mnthly_end_dt, ean

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.dim_meter_stg ;

# COMMAND ----------

display(spark.sql('select * from retta.dim_meter_relation_stg'))

# COMMAND ----------

select
  building_id,
  (sum(main_units) - sum(sub_units)) as net_units,
  mnthly_strt_dt,
  mnthly_end_dt
from
  cte_join
group by
  building_id,
  mnthly_strt_dt,
  mnthly_end_dt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.energy_monthly_ref where ean = '80040887624600475931'

# COMMAND ----------

# MAGIC %sql with cte_join as (
# MAGIC   select
# MAGIC     mtr.meter_ID,
# MAGIC     building_ID,
# MAGIC     mtr.ean_num,
# MAGIC     energy.mnthly_strt_dt,
# MAGIC     energy.mnthly_end_dt,
# MAGIC     energy.reading_dt,
# MAGIC     energy.mnthly_units as main_units,
# MAGIC    -- enrgy_a.mnthly_units as sub_units,
# MAGIC     sub_meter_id
# MAGIC   from
# MAGIC     retta.dim_meter_stg mtr
# MAGIC     join retta.energy_monthly_tgt_stg energy on mtr.ean_num = energy.ean
# MAGIC     join retta.dim_meter_relation_stg mtr_reln on mtr.ean_num = mtr_reln.meter_id
# MAGIC     --left join retta.energy_monthly_tgt_stg enrgy_a on mtr_reln.sub_meter_id = enrgy_a.ean
# MAGIC ), cte_new as 
# MAGIC  (select a.*, enrgy_a.mnthly_units as sub_units from cte_join a left join retta.energy_monthly_tgt_stg enrgy_a on a.sub_meter_id = enrgy_a.ean
# MAGIC  and a.mnthly_end_dt = enrgy_a.mnthly_end_dt) 
# MAGIC  select a.*, main_units-sub_units as net from cte_new a where ean_num='20003317'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.energy_monthly_tgt_stg where ean in ('20003317', '20003298') and mnthly_end_dt in('2018-12-01', '2019-01-01')
