--
-- Author: José F. E. Almeida
-- 04/2022 - 08/2022
--

CREATE TABLE `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.agg_cp_data_fmcg_2019_2020`
PARTITION BY datep
CLUSTER BY hhkey, brand AS
SELECT rd.hhkey, brand,
    PARSE_DATE("%Y%m%d", datep) AS datep,
    value * brandfactor * hh.continuo AS spent_cs, -- Continuous Sample value (with hh weight applied)
    value * brandfactor * rwkorr * rwstd * fullmasW AS spent_fs, -- Full Sample value (all weights applied)
FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020` rd
JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020` hh
ON rd.hhkey = hh.hhkey