SELECT
  quality_status AS status_qualidade,
  latest_updated_at AS atualizado_em,
  message AS mensagem
FROM public.vw_finance_data_quality_banner;

