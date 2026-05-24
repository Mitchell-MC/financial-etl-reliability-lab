with latest_run as (
  select
    run_id,
    status,
    start_ts,
    end_ts,
    records_ingested,
    records_silver,
    records_quarantine,
    freshness_lag_minutes,
    error_message,
    date_diff('second', start_ts, end_ts) as duration_seconds
  from pipeline_runs
  order by end_ts desc
  limit 1
),
run_stats_7d as (
  select
    count(*) as runs_last_7d,
    sum(case when status = 'success' then 1 else 0 end) as successful_runs_last_7d,
    round(avg(records_quarantine), 2) as avg_quarantine_last_7d,
    round(avg(freshness_lag_minutes), 2) as avg_freshness_lag_last_7d,
    round(avg(date_diff('second', start_ts, end_ts)), 2) as avg_duration_seconds_last_7d
  from pipeline_runs
  where end_ts >= current_timestamp - interval 7 day
),
quarantine_breakdown as (
  select
    quarantine_reason,
    count(*) as issue_count
  from quarantine_transactions
  group by quarantine_reason
),
quarantine_top as (
  select
    string_agg(concat(quarantine_reason, ':', issue_count), ', ' order by issue_count desc) as top_quarantine_reasons
  from quarantine_breakdown
)
select
  lr.run_id,
  lr.status as latest_status,
  lr.duration_seconds,
  lr.records_ingested,
  lr.records_silver,
  lr.records_quarantine,
  lr.freshness_lag_minutes,
  rs.runs_last_7d,
  rs.successful_runs_last_7d,
  case
    when rs.runs_last_7d = 0 then null
    else round((rs.successful_runs_last_7d::double / rs.runs_last_7d::double) * 100, 2)
  end as success_rate_last_7d,
  rs.avg_quarantine_last_7d,
  rs.avg_freshness_lag_last_7d,
  rs.avg_duration_seconds_last_7d,
  qt.top_quarantine_reasons,
  lr.error_message
from latest_run lr
cross join run_stats_7d rs
cross join quarantine_top qt;
