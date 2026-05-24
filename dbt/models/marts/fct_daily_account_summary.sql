select
  cast(posted_at as date) as run_date,
  account_id,
  currency,
  round(sum(amount), 2) as total_amount,
  count(*) as transaction_count,
  max(refreshed_at) as last_refresh_ts
from {{ ref('stg_silver_transactions') }}
group by 1, 2, 3
