CREATE TABLE `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.cp_data_fmcg_2019_2020`
PARTITION BY datep
CLUSTER BY hhkey, brand AS
SELECT hhkey, occaskey, brandfactor, rwkorr, rwstd, PARSE_DATE("%Y%m%d", datep) AS datep, recid, value, fullmasW, continuo, brand
FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020`