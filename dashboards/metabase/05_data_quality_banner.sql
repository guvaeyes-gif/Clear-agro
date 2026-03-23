SELECT
  quality_status AS status_qualidade,
  latest_updated_at AS atualizado_em,
  unknown_ap_rows AS linhas_ap_desconhecidas,
  unknown_ar_rows AS linhas_ar_desconhecidas,
  has_ap_cz AS possui_ap_cz,
  has_ap_cr AS possui_ap_cr,
  has_ar_cz AS possui_ar_cz,
  has_ar_cr AS possui_ar_cr,
  message AS mensagem
FROM public.vw_finance_data_quality_banner;

