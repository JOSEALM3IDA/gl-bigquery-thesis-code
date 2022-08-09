--
-- Author: José F. E. Almeida
-- 04/2022 - 08/2022
--

DECLARE d_start_p1 DATE DEFAULT DATE("2019-01-01");
DECLARE d_end_p1 DATE DEFAULT DATE("2019-12-31");
DECLARE d_start_p2 DATE DEFAULT DATE("2020-01-01");
DECLARE d_end_p2 DATE DEFAULT DATE("2020-12-31");

CREATE OR REPLACE PROCEDURE cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_cp_oj(
  start_p1 DATE, end_p1 DATE, start_p2 DATE, end_p2 DATE)
BEGIN

WITH
table_raw_data_period AS (
    -- Query was nested due to the step of separating the periods, but this is not necessary
    SELECT hhkey,
        CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS brand,
        value * brandfactor AS value,
        rwkorr, rwstd, fullmasW,
        CASE WHEN datep >= start_p1 AND datep <= end_p1 THEN 1 ELSE 2 END AS period
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.cp_data_fmcg_2019_2020` rawfmcg
    LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
    ON rawfmcg.brand = ba.brand
    WHERE datep >= start_p1 AND datep <= end_p1 OR datep >= start_p2 AND datep <= end_p2
),
table_raw_households AS (
    SELECT hhkey, continuo
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020`
),

-- Data Input
--------------------------
-- Pre-run

table_hh_brand_period_rp AS (
    SELECT hhkey, brand,
        SUM(_spent_p1_ics) AS spent_p1_ics,
        SUM(_spent_p2_ics) AS spent_p2_ics,
        SUM(_spent_p1_fs) AS spent_p1_fs,
        SUM(_spent_p2_fs) AS spent_p2_fs,
    FROM (
        -- Separate the values according to period before doing the SUM
        -- This makes it easier to read the query and should marginally improve performance
        SELECT hhkey, brand,
            CASE WHEN period = 1 THEN value ELSE 0 END AS _spent_p1_ics,
            CASE WHEN period = 2 THEN value ELSE 0 END AS _spent_p2_ics,
            CASE WHEN period = 1 THEN value * rwkorr * rwstd * fullmasW ELSE 0 END AS _spent_p1_fs,
            CASE WHEN period = 2 THEN value * rwkorr * rwstd * fullmasW ELSE 0 END AS _spent_p2_fs
        FROM table_raw_data_period
    )
    GROUP BY hhkey, brand
),
table_hh_period_rp AS (
    -- CTE is already very similar to the new structure from the above CTE, so we don't change this one
    SELECT sumhh.hhkey, brand,
        sumhh.spent_p1_ics * rawhh.continuo AS spent_p1_cs,
        sumhh.spent_p2_ics * rawhh.continuo AS spent_p2_cs,
        spent_p1_fs, spent_p2_fs
    FROM table_raw_households rawhh, (
        SELECT hhkey, brand,
            SUM(spent_p1_ics) AS spent_p1_ics, SUM(spent_p2_ics) AS spent_p2_ics,
            SUM(spent_p1_fs) AS spent_p1_fs, SUM(spent_p2_fs) AS spent_p2_fs
        FROM table_hh_brand_period_rp
        GROUP BY hhkey, brand
    ) sumhh
    WHERE sumhh.hhkey = rawhh.hhkey
),
table_pre_run AS (
    SELECT brand,
        IFNULL(SAFE_DIVIDE(brand_sales_p1_fs, brand_sales_p1_cs), 0) AS corr_factor_p1,
        IFNULL(SAFE_DIVIDE(brand_sales_p2_fs, brand_sales_p2_cs), 0) AS corr_factor_p2
    FROM (
        SELECT brand,
            SUM(spent_p1_cs) AS brand_sales_p1_cs, SUM(spent_p2_cs) AS brand_sales_p2_cs,
            SUM(spent_p1_fs) AS brand_sales_p1_fs, SUM(spent_p2_fs) AS brand_sales_p2_fs
        FROM table_hh_period_rp
        GROUP BY brand
    )
),

-- Pre-run
--------------------------
-- Main-run

table_hh_period AS (
    SELECT hhkey, thh.brand,
        spent_p1_cs * corr_factor_p1 AS spent_p1,
        spent_p2_cs * corr_factor_p2 AS spent_p2
    FROM table_hh_period_rp thh
    JOIN table_pre_run tcorr
    ON thh.brand = tcorr.brand
    WHERE spent_p1_cs != 0 OR spent_p2_cs != 0
),
table_hh_brand_cseg AS (
    SELECT hhkey, brand, spent_p1, spent_p2,
        spent_p1 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p1), 0) AS cseg_p1,
        spent_p2 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p2), 0) AS cseg_p2,
        cseg
    FROM (
      SELECT hhkey, brand, spent_p1, spent_p2, total_spent_p1, total_spent_p2, LEAST(total_spent_p1, total_spent_p2) AS cseg
        FROM (
            SELECT hhkey, brand, spent_p1, spent_p2,
                SUM(spent_p1) OVER(PARTITION BY hhkey) AS total_spent_p1,
                SUM(spent_p2) OVER(PARTITION BY hhkey) AS total_spent_p2
            FROM table_hh_period
        )
    )
),
table_hh_gl_b2b AS (
    SELECT hhkey, b1.brand AS B1, b2.brand AS B2,
        b2.cseg_p1 * IFNULL(SAFE_DIVIDE(b1.cseg_p2, cseg), 0) AS hh_gains_b1_to_b2,
        b1.cseg_p1 * IFNULL(SAFE_DIVIDE(b2.cseg_p2, cseg), 0) AS hh_losses_b1_to_b2
    FROM (
        SELECT hhkey, brand, cseg_p1, cseg_p2, cseg, 
            ARRAY_AGG(STRUCT(brand, cseg_p1, cseg_p2)) OVER(PARTITION BY hhkey) brands
        FROM table_hh_brand_cseg
  ) b1, UNNEST(brands) AS b2  
)

SELECT B1, B2, ROUND(SUM(hh_gains_b1_to_b2), 2) as total_gains_b1_to_b2, ROUND(SUM(hh_losses_b1_to_b2), 2) AS total_losses_b1_to_b2
FROM table_hh_gl_b2b tgl
GROUP BY B1, B2
ORDER BY B1, B2;

END;

CALL cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_cp_oj(d_start_p1, d_end_p1, d_start_p2, d_end_p2);