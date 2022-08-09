--
-- Author: José F. E. Almeida
-- 04/2022 - 08/2022
--

CREATE TABLE `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg` (
  brand STRING NOT NULL,
  aggregation STRING NOT NULL
);

INSERT INTO `gfk-science-prod-gold.cp_gain_loss_bachelor_thesis.brand_aggregation_fmcg`
VALUES ('KAUFLAND', 'KAUFLAND'),
  ('MARKTKAUF', 'MARKTKAUF'),
  ('REAL', 'REAL'),
  ('REWE', 'REWE'),
  ('EDEKA EH', 'EDEKA EH'),
  ('GLOBUS ST. WENDEL', 'GLOBUS ST. WENDEL'),
  ('TENGELMANN/KAISERS', 'TENGELMANN/KAISERS'),
  ('REST LEH-VOLLSORTIMENTER FOOD', 'REST LEH-VOLLSORTIMENTER FOOD'),
  ('ALDI', 'ALDI'),
  ('LIDL', 'LIDL'),
  ('NETTO MARKEN-DISCOUNT', 'NETTO MARKEN-DISCOUNT'),
  ('NORMA', 'NORMA'),
  ('PENNY', 'PENNY'),
  ('PLUS', 'PLUS'),
  ('REST DISCOUNTER', 'REST DISCOUNTER'),
  ('DM', 'DM'),
  ('MUELLER', 'MUELLER'),
  ('ROSSMANN', 'ROSSMANN'),
  ('SCHLECKER', 'SCHLECKER'),
  ('REST DROGERIEMAERKTE', 'REST DROGERIEMAERKTE'),

  ('FACHHANDEL', 'FACHHANDEL'),
  ('FACHHANDEL NICHT STAT. SONST.', 'FACHHANDEL'),
  ('FACHHANDEL STATIONAER', 'FACHHANDEL');
