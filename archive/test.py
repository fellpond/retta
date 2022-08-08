# Databricks notebook source
# MAGIC %sql
# MAGIC select * from retta.dim_site_stg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *  from retta.energy_monthly_stg where ean = '20003317' order by reading_dt desc;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *  from retta.energy_monthly_tgt_stg where ean = '20003317';

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table retta.energy_monthly_stg;
# MAGIC truncate table retta.energy_monthly_tgt_stg;
# MAGIC truncate table retta.energy_monthly_ref;
# MAGIC truncate table retta.energy_dly_stg;
# MAGIC truncate table retta.energy_dly_tgt_stg;
# MAGIC truncate table retta.energy_dly_ref;

# COMMAND ----------

display(spark.sql("select * from retta.energy_monthly_stg where ean = 20003317 order by ean, mnthly_end_dt"))
#display(spark.sql("select * from retta.energy_monthly_tgt_stg"))
#display(spark.sql("select * from retta.energy_monthly_ref"))
# #display(spark.sql("select * from retta.energy_dly_stg"))
# display(spark.sql("select * from retta.energy_dly_tgt_stg"))
# display(spark.sql("select * from retta.energy_dly_ref"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC select building_id,mnthly_strt_dt, mnthly_end_dt, yrmnth, sum(net_units) over (partition by building_id order by mnthly_end_dt rows between 11 preceding and current row ) as rolling_12m_units , net_units from retta.energy_monthly_building order by building_ID, mnthly_end_dt ;

# COMMAND ----------


display(spark.sql("select * from retta.energy_monthly_ref"))
display(spark.sql("select * from retta.energy_dly_ref"))
display(spark.sql("select * from retta.energy_dly_tgt_stg order by "))
display(spark.sql("select * from retta.energy_monthly_tgt_stg"))

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table retta.energy_monthly_tgt_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.energy_monthly_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from retta.dim_investment_proposal_stg
