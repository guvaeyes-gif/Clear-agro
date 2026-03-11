SELECT
  quality_status,
  latest_updated_at,
  unknown_ap_rows,
  unknown_ar_rows,
  has_ap_cz,
  has_ap_cr,
  has_ar_cz,
  has_ar_cr,
  message
FROM public.vw_finance_data_quality_banner;

