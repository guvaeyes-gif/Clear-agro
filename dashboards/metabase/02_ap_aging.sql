SELECT
  company AS empresa,
  aging_bucket AS faixa,
  titles_count AS titulos,
  amount_total AS valor_total,
  amount_pct AS valor_percentual
FROM public.vw_finance_ap_aging
WHERE company = {{company}}
ORDER BY
  CASE aging_bucket
    WHEN '0-30' THEN 1
    WHEN '31-60' THEN 2
    WHEN '61-90' THEN 3
    WHEN '>90' THEN 4
    WHEN 'a_vencer_30' THEN 5
    WHEN 'a_vencer_mais_30' THEN 6
    ELSE 99
  END;

