select
  natural_key,
  source_name,
  raw_transaction_id,
  account_id,
  cast(posted_at as timestamp) as posted_at,
  cast(amount as double) as amount,
  upper(currency) as currency,
  description,
  cast(updated_at as timestamp) as updated_at,
  status,
  load_run_id,
  cast(refreshed_at as timestamp) as refreshed_at
from silver_transactions
where amount is not null
