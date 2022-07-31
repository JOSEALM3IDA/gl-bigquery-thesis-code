CREATE OR REPLACE PROCEDURE cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_daterange_2(
  start_p1 DATE, end_p1 DATE, start_p2 DATE, end_p2 DATE)
BEGIN

WITH
table_raw_data_period AS (
    SELECT hhkey,
        CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS marke,
        spent_cont, spent_fsw,
        CASE WHEN datum >= start_p1 AND datum <= end_p1 THEN 1
        ELSE 2 END AS period
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.data_fmcg_2019_2020`
    LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
    ON marke = ba.brand
    WHERE (datum >= start_p1 AND datum <= end_p1) OR (datum >= start_p2 AND datum <= end_p2) # Pushing filter down
),
table_hh_brand_period_rp AS (
    SELECT hhkey, marke,
        SUM(ispent_p1_cont) AS spent_p1_cont,
        SUM(ispent_p2_cont) AS spent_p2_cont,
        SUM(ispent_p1_fsw) AS spent_p1_fsw,
        SUM(ispent_p2_fsw) AS spent_p2_fsw
    FROM (
        SELECT hhkey, marke,
            CASE WHEN period = 1 THEN spent_cont ELSE 0 END AS ispent_p1_cont,
            CASE WHEN period = 2 THEN spent_cont ELSE 0 END AS ispent_p2_cont,
            CASE WHEN period = 1 THEN spent_fsw ELSE 0 END AS ispent_p1_fsw,
            CASE WHEN period = 2 THEN spent_fsw ELSE 0 END AS ispent_p2_fsw,
        FROM table_raw_data_period
    )
    GROUP BY hhkey, marke
),
table_rp_factor AS (
    SELECT marke,
        IFNULL(SAFE_DIVIDE(SUM(spent_p1_fsw), SUM(spent_p1_cont)), 0) AS factor_p1,
        IFNULL(SAFE_DIVIDE(SUM(spent_p2_fsw), SUM(spent_p2_cont)), 0) AS factor_p2
    FROM table_hh_brand_period_rp
    GROUP BY marke
),
table_hh_period AS (
    SELECT hhkey, thhrp.marke,
        spent_p1_cont * factor_p1 AS spent_p1,
        spent_p2_cont * factor_p2 AS spent_p2
    FROM table_hh_brand_period_rp thhrp
    JOIN table_rp_factor trpf ON thhrp.marke = trpf.marke
    WHERE spent_p1_cont != 0 OR spent_p2_cont != 0
),
table_hh_brand_cseg AS (
    SELECT hhkey, marke, cseg,
        spent_p1 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p1), 0) AS cseg_p1,
        spent_p2 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p2), 0) AS cseg_p2
    FROM (
        SELECT hhkey, marke, spent_p1, spent_p2, total_spent_p1, total_spent_p2, LEAST(total_spent_p1, total_spent_p2) AS cseg
        FROM (
            SELECT hhkey, marke, spent_p1, spent_p2,
                SUM(spent_p1) OVER(PARTITION BY hhkey) AS total_spent_p1,
                SUM(spent_p2) OVER(PARTITION BY hhkey) AS total_spent_p2
            FROM table_hh_period
        )
    )
),
table_hh_gl_b2b AS (
    SELECT hhkey, t.marke as b1, brand.marke as b2,
      brand.cseg_p1 * IFNULL(SAFE_DIVIDE(t.cseg_p2, cseg), 0) AS hh_gains_b1_to_b2,
      t.cseg_p1 * IFNULL(SAFE_DIVIDE(brand.cseg_p2, cseg), 0) AS hh_losses_b1_to_b2
    FROM (
      SELECT hhkey, marke, cseg_p1, cseg_p2, cseg, 
        ARRAY_AGG(STRUCT(marke, cseg_p1, cseg_p2)) OVER(PARTITION BY hhkey) brands
      FROM table_hh_brand_cseg
  ) t, UNNEST(brands) AS brand  
)

SELECT B1, B2, ROUND(SUM(hh_gains_b1_to_b2), 2) as total_gains_b1_to_b2, ROUND(SUM(hh_losses_b1_to_b2), 2) as total_losses_b1_to_b2
FROM table_hh_gl_b2b tgl
GROUP BY B1, B2
ORDER BY B1, B2;

END