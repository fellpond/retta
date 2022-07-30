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
# MAGIC     left join retta.energy_monthly_tgt_stg enrgy_a on mtr_reln.sub_meter_id = enrgy_a.ean
# MAGIC )
# MAGIC select * from 
# MAGIC -- select building_ID, mnthly_strt_dt, mnthly_end_dt, (sum(main_units) - sum((case when cte.sub_units is null then 0 else cte.sub_units end))) as net_units from cte_join cte group by building_ID, mnthly_strt_dt, mnthly_end_dt;

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
