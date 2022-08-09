--
-- Author: José F. E. Almeida
-- 04/2022 - 08/2022
--

-- NOTE: When analyzing complex SQL queries, I start with understanding the subqueries. Otherwise, they will be harder to understand.
-- This is reflected in the comments I've written - if they're read in another order, they might not make much sense.

-- Period 1: 2019
-- Period 2: 2020
DECLARE d_start_p1 DATE DEFAULT DATE("2019-01-01");
DECLARE d_end_p1 DATE DEFAULT DATE("2019-12-31");
DECLARE d_start_p2 DATE DEFAULT DATE("2020-01-01");
DECLARE d_end_p2 DATE DEFAULT DATE("2020-12-31");

CREATE OR REPLACE PROCEDURE cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_prototype(
  start_p1 DATE, end_p1 DATE, start_p2 DATE, end_p2 DATE)
BEGIN

WITH
table_hh_brand_period_rp AS (
    /* First step of compressing the data: having the period as a column makes it more complex (and slower) to find and work with entries for each household from different periods.
    The solution found was to "transpose" these values, inserting all the different values for each period into multiple columns of the same entry.
    These different values per period are promptly available when they need to be used together.
    In this first step, the values are not yet transposed, but are split in different columns, writing zeros when the values don't refer to that specific period.
    These columns can later be summed together, obtaining the desired table. */
    SELECT hhkey, brand,
        CASE WHEN period = 1 THEN SUM(value * brandfactor) ELSE 0 END AS spent_p1_ics, -- Intermediate Continuous Sample value, Period 1
        CASE WHEN period = 2 THEN SUM(value * brandfactor) ELSE 0 END AS spent_p2_ics, -- Intermediate Continuous Sample value, Period 2
        CASE WHEN period = 1 THEN SUM(value * brandfactor * rwkorr * rwstd * fullmasW) ELSE 0 END AS spent_p1_fs, -- Full Sample value, Period 1
        CASE WHEN period = 2 THEN SUM(value * brandfactor * rwkorr * rwstd * fullmasW) ELSE 0 END AS spent_p2_fs, -- Full Sample value, Period 2
    FROM (
        -- Filtering values according to date, deciding if each value belongs to the 1st or 2nd period
        SELECT hhkey, brand, value, brandfactor, rwkorr, rwstd, fullmasW,
            CASE WHEN datep >= start_p1 AND datep <= end_p1 THEN 1
            ELSE CASE WHEN datep >= start_p2 AND datep <= end_p2 THEN 2
            ELSE 3 END END AS period
        FROM (
            -- Directly reading necessary columns from the raw data, applying brand aggregation applying brand aggregation and selection (which are parameterized in another table)
            SELECT hhkey,
                CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS brand,
                value, brandfactor, rwkorr, rwstd, fullmasW, PARSE_DATE("%Y%m%d", datep) AS datep
            FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020` rawfmcg
            LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
            ON rawfmcg.brand = ba.brand
        )
        
    )
    GROUP BY hhkey, brand, period
),
table_hh_period_rp AS (
    -- Applying the continuous weights on the intermediate continuous values, obtaining the continuous sample values per household and brand combinations
    SELECT sumhh.hhkey, brand,
        sumhh.spent_p1_ics * rawhh.continuo AS spent_p1_cs,
        sumhh.spent_p2_ics * rawhh.continuo AS spent_p2_cs,
        spent_p1_fs, spent_p2_fs
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020` rawhh, (
        -- Obtain the weights grouped by household, brand and period (equivalent to table 2.2, but compressed/transposed like above explained)
        SELECT hhkey, brand,
            SUM(spent_p1_ics) AS spent_p1_ics, SUM(spent_p2_ics) AS spent_p2_ics,
            SUM(spent_p1_fs) AS spent_p1_fs, SUM(spent_p2_fs) AS spent_p2_fs
        FROM table_hh_brand_period_rp
        GROUP BY hhkey, brand
    ) sumhh
    WHERE sumhh.hhkey = rawhh.hhkey
),
table_pre_run AS (
    -- Calculating the correction factor per brand and period (table 2.3)
    SELECT brand,
        IFNULL(SAFE_DIVIDE(brand_sales_p1_fs, brand_sales_p1_cs), 0) AS corr_factor_p1,
        IFNULL(SAFE_DIVIDE(brand_sales_p2_fs, brand_sales_p2_cs), 0) AS corr_factor_p2
    FROM (
        -- Removing household grouping in preparation for the final per-brand calculation
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

table_hh_brand_period AS (
    -- Applying the correction factor from the pre-run on the main-run, upgrading it to the RP run
    SELECT hhkey, trd.brand,
        CASE WHEN period = 1 THEN SUM(value * brandfactor * corr_factor_p1) ELSE 0 END AS spent_p1,
        CASE WHEN period = 2 THEN SUM(value * brandfactor * corr_factor_p2) ELSE 0 END AS spent_p2
    FROM (
        -- Separate values according to date, deciding if each value belongs to the 1st or 2nd period (again)
        SELECT hhkey, brand, value, brandfactor, rwkorr, rwstd, fullmasW,
            CASE WHEN datep >= start_p1 AND datep <= end_p1 THEN 1
            ELSE CASE WHEN datep >= start_p2 AND datep <= end_p2 THEN 2
            ELSE 3 END END AS period
        FROM (
            -- Directly reading necessary columns from the raw data (again), applying brand aggregation and selection (which are parameterized in another table)
            SELECT hhkey,
                CASE WHEN ba.brand IS NOT NULL THEN ba.aggregation ELSE 'REST' END AS brand,
                value, brandfactor, rwkorr, rwstd, fullmasW, PARSE_DATE("%Y%m%d", datep) AS datep
            FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_fmcg_2019_2020` rawfcmg
            LEFT OUTER JOIN `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` ba
            ON rawfcmg.brand = ba.brand
        )
    ) trd
    JOIN table_pre_run trp ON trd.brand = trp.brand
    GROUP BY hhkey, brand, period
),
table_hh_period AS (
    -- Applying the continuous sample weights (table 2.4). This is done separately to make the query easier to understand.
    -- Theoretically, the weights should be applied at the same time as the corr_factor, but in practice it doesn't matter, due to the distributive property of multiplication
    SELECT sumhh.hhkey, brand, sumhh.spent_p1 * rawhh.continuo AS spent_p1, sumhh.spent_p2 * rawhh.continuo AS spent_p2
    FROM `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.raw_data_households_2019_2020` rawhh, (
        SELECT hhkey, brand, SUM(spent_p1) AS spent_p1, SUM(spent_p2) AS spent_p2
        FROM table_hh_brand_period
        GROUP BY hhkey, brand
    ) sumhh
    WHERE sumhh.hhkey = rawhh.hhkey
    AND rawhh.continuo != 0
),
table_hh_brand_sums AS (
    -- Calculate household CSEG (table 2.5)
    SELECT hhkey, total_spent_p1, total_spent_p2, LEAST(total_spent_p1, total_spent_p2) AS cseg
    FROM (
        -- Total spent per household and period
        SELECT hhkey, SUM(spent_p1) AS total_spent_p1, SUM(spent_p2) AS total_spent_p2
        FROM table_hh_period
        GROUP BY hhkey
    ) table_sums
),
table_hh_brand_cseg AS (
    -- Calculate CSEG per household, brand and period (table 2.6)
    SELECT tper.hhkey, brand, spent_p1, spent_p2,
        spent_p1 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p1), 0) AS cseg_p1,
        spent_p2 * IFNULL(SAFE_DIVIDE(cseg, total_spent_p2), 0) AS cseg_p2
    FROM table_hh_period tper
    JOIN table_hh_brand_sums tsums
    ON tper.hhkey = tsums.hhkey
),
table_hh_gl_b2b AS (
    -- Gains and losses from all brands to all other brands, per household (table 2.7)
    -- Self-join for comparing brands to each other (in the same household)
    SELECT b1.hhkey, b1.brand AS B1, b2.brand AS B2,
        b2.cseg_p1 * IFNULL(SAFE_DIVIDE(b1.cseg_p2, cseg), 0) AS hh_gains_b1_to_b2,
        b1.cseg_p1 * IFNULL(SAFE_DIVIDE(b2.cseg_p2, cseg), 0) AS hh_losses_b1_to_b2
    FROM table_hh_brand_cseg b1
    JOIN table_hh_brand_cseg b2
    ON b1.hhkey = b2.hhkey
    JOIN table_hh_brand_sums tsums
    ON b1.hhkey = tsums.hhkey
)

-- Final gains and losses from all brands to all other brands (table 2.8)
SELECT B1, B2, ROUND(SUM(hh_gains_b1_to_b2), 2) as total_gains_b1_to_b2, ROUND(SUM(hh_losses_b1_to_b2), 2) AS total_losses_b1_to_b2
FROM table_hh_gl_b2b tgl
GROUP BY B1, B2
ORDER BY B1, B2;

END;

CALL cp_gain_loss_bachelor_thesis.calc_gain_loss_rp_fmcg_prototype(d_start_p1, d_end_p1, d_start_p2, d_end_p2);