DECLARE start_p1 DATE DEFAULT DATE("2019-01-01");
DECLARE end_p1 DATE DEFAULT DATE("2019-12-31");
DECLARE start_p2 DATE DEFAULT DATE("2020-01-01");
DECLARE end_p2 DATE DEFAULT DATE("2020-12-31");

CREATE OR REPLACE PROCEDURE cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_prototype(
  start_p1 DATE, end_p1 DATE, start_p2 DATE, end_p2 DATE)
BEGIN

WITH
-- Transitional step, obtaining the weights grouped by the household, brand and period (equivalent to table 2.2)
-- Also separates the values according to date, deciding if each value belongs to the 1st or 2nd period
table_hh_brand_period_rp AS (
    SELECT hhkey, marke,
        CASE WHEN period = 1 THEN SUM(wert * markenfactor) ELSE 0 END AS ispent_p1_cont,
        CASE WHEN period = 2 THEN SUM(wert * markenfactor) ELSE 0 END AS ispent_p2_cont,
        CASE WHEN period = 1 THEN SUM(wert * markenfactor * rwkorr * rwstd * fullmasW) ELSE 0 END AS ispent_p1_fsw,
        CASE WHEN period = 2 THEN SUM(wert * markenfactor * rwkorr * rwstd * fullmasW) ELSE 0 END AS ispent_p2_fsw,
    FROM (
        SELECT hhkey, marke, wert, markenfactor, rwkorr, rwstd, fullmasW,
            CASE WHEN datum >= start_p1 AND datum <= end_p1 THEN 1
            ELSE CASE WHEN datum >= start_p2 AND datum <= end_p2 THEN 2
            ELSE 3 END END AS period
        FROM (
            SELECT hhkey,
                CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS marke,
                wert, markenfactor, rwkorr, rwstd, fullmasW, PARSE_DATE("%Y%m%d", datum) AS datum
            FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020`
            LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
            ON marke = ba.brand
        )
        
    )
    GROUP BY hhkey, marke, period
),
table_hh_period_rp AS (
    SELECT sumhh.hhkey, marke,
        sumhh.spent_p1_cont * rawhh.continuo AS spent_p1_cont, sumhh.spent_p2_cont * rawhh.continuo AS spent_p2_cont,
        spent_p1_fsw, spent_p2_fsw
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020` rawhh, (
        SELECT hhkey, marke,
            SUM(ispent_p1_cont) AS spent_p1_cont, SUM(ispent_p2_cont) AS spent_p2_cont,
            SUM(ispent_p1_fsw) AS spent_p1_fsw, SUM(ispent_p2_fsw) AS spent_p2_fsw
        FROM table_hh_brand_period_rp
        GROUP BY hhkey, marke
    ) sumhh
    WHERE sumhh.hhkey = rawhh.hhkey
),
table_pre_run AS (
    SELECT marke,
        IFNULL(SAFE_DIVIDE(brand_sales_p1_fsw, brand_sales_p1_cont), 0) AS factor_p1,
        IFNULL(SAFE_DIVIDE(brand_sales_p2_fsw, brand_sales_p2_cont), 0) AS factor_p2
    FROM (
        SELECT marke,
            SUM(spent_p1_cont) AS brand_sales_p1_cont, SUM(spent_p2_cont) AS brand_sales_p2_cont,
            SUM(spent_p1_fsw) AS brand_sales_p1_fsw, SUM(spent_p2_fsw) AS brand_sales_p2_fsw
        FROM table_hh_period_rp
        GROUP BY marke
    )
),

-- Pre run
--------------------------
-- Main run

table_hh_brand_period AS (
    SELECT hhkey, trd.marke,
        CASE WHEN period = 1 THEN SUM(wert * markenfactor * factor_p1) ELSE 0 END AS ispent_p1,
        CASE WHEN period = 2 THEN SUM(wert * markenfactor * factor_p2) ELSE 0 END AS ispent_p2
    FROM (
        SELECT hhkey, marke, wert, markenfactor, rwkorr, rwstd, fullmasW,
            CASE WHEN datum >= start_p1 AND datum <= end_p1 THEN 1
            ELSE CASE WHEN datum >= start_p2 AND datum <= end_p2 THEN 2
            ELSE 3 END END AS period
        FROM (
            SELECT hhkey,
                CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS marke,
                wert, markenfactor, rwkorr, rwstd, fullmasW, PARSE_DATE("%Y%m%d", datum) AS datum
            FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020`
            LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
            ON marke = ba.brand
        )
    ) trd
    JOIN table_pre_run trp ON trd.marke = trp.marke
    GROUP BY hhkey, marke, period
),
table_hh_period AS (
    SELECT sumhh.hhkey, marke, sumhh.spent_p1 * rawhh.continuo AS spent_p1, sumhh.spent_p2 * rawhh.continuo AS spent_p2
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020` rawhh, (
        SELECT hhkey, marke, SUM(ispent_p1) AS spent_p1, SUM(ispent_p2) AS spent_p2
        FROM table_hh_brand_period
        GROUP BY hhkey, marke
    ) sumhh
    WHERE sumhh.hhkey = rawhh.hhkey
    AND rawhh.continuo != 0
),
table_hh_brand_sums AS (
    SELECT hhkey, total_spent_p1, total_spent_p2, LEAST(total_spent_p1, total_spent_p2) AS cseg
    FROM (
        SELECT hhkey, SUM(spent_p1) AS total_spent_p1, SUM(spent_p2) AS total_spent_p2
        FROM table_hh_period
        GROUP BY hhkey
    ) table_sums
),
table_hh_brand_cseg AS (
    SELECT tper.hhkey, marke, spent_p1, spent_p2,
        spent_p1 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p1), 0) AS cseg_p1,
        spent_p2 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p2), 0) AS cseg_p2
    FROM table_hh_period tper
    JOIN table_hh_brand_sums tsums
    ON tper.hhkey = tsums.hhkey
),
table_hh_gl_b2b AS (
    SELECT b1.hhkey, b1.marke AS B1, b2.marke AS B2,
        b2.cseg_p1 * IFNULL(SAFE_DIVIDE(b1.cseg_p2, cseg), 0) AS hh_gains_b1_to_b2,
        b1.cseg_p1 * IFNULL(SAFE_DIVIDE(b2.cseg_p2, cseg), 0) AS hh_losses_b1_to_b2
    FROM table_hh_brand_cseg b1
    JOIN table_hh_brand_cseg b2
    ON b1.hhkey = b2.hhkey
    JOIN table_hh_brand_sums tsums
    ON b1.hhkey = tsums.hhkey
)

SELECT B1, B2, ROUND(SUM(hh_gains_b1_to_b2), 2) as total_gains_b1_to_b2, ROUND(SUM(hh_losses_b1_to_b2), 2) AS total_losses_b1_to_b2
FROM table_hh_gl_b2b tgl
GROUP BY B1, B2
ORDER BY B1, B2;

END;

CALL cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_prototype(start_p1, end_p1, start_p2, end_p2);