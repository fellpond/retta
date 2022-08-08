# Databricks notebook source
building_df = spark.table('retta.dim_building_stg')
meter_df = spark.table('retta.dim_meter_stg')
meter_reln_df = spark.table('retta.dim_meter_relation_stg')
energy_monthly_building='/mnt/silver/energy_monthly_building'
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
# MAGIC     and energy.mnthly_end_dt = enrgy_a.mnthly_end_dt
# MAGIC )
# MAGIC select
# MAGIC   building_ID,
# MAGIC   mnthly_strt_dt,
# MAGIC   mnthly_end_dt,
# MAGIC   concat(
# MAGIC     year(mnthly_end_dt),
# MAGIC     right(concat('0', month(mnthly_end_dt)), 2)
# MAGIC   ) as yrmnth,
# MAGIC   (
# MAGIC     sum(distinct(main_units)) - sum(
# MAGIC       (
# MAGIC         case
# MAGIC           when cte.sub_units is null then 0
# MAGIC           else cte.sub_units
# MAGIC         end
# MAGIC       )
# MAGIC     )
# MAGIC   ) as net_units
# MAGIC from
# MAGIC   cte_join cte
# MAGIC group by
# MAGIC   building_ID,
# MAGIC   mnthly_strt_dt,
# MAGIC   mnthly_end_dt;

# COMMAND ----------

_sqldf.write.format('delta').mode('append').option('path', energy_monthly_building).saveAsTable('retta.energy_monthly_building')

# COMMAND ----------

rl_12_mnth = spark.sql('select building_id,mnthly_strt_dt, mnthly_end_dt, yrmnth, sum(net_units) over (partition by building_id order by mnthly_end_dt rows between 11 preceding and current row ) as rolling_12m_units , net_units from retta.energy_monthly_building order by building_ID, mnthly_end_dt')

energy_12mnth='/mnt/silver/energy_rolling12m'

rl_12_mnth.write.format('parquet').mode('overwrite').option('path', energy_12mnth).saveAsTable('retta.energy_rolling12m')
