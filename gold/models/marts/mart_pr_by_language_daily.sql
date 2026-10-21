-- grain: language x day. A PR sends several events, so counts are by action
select
    f.date_key,
    d.full_date,
    f.repo_language,
    count_if(f.action = 'opened')                                    as prs_opened,
    count_if(f.action = 'closed' and f.is_merged)                    as prs_merged,
    count_if(f.action = 'opened' and f.pr_author_is_bot)             as bot_prs_opened,
    round(avg(case when f.action = 'closed' and f.is_merged then f.merge_time_hours end), 2) as avg_merge_time_hours,
    round(avg(case when f.action = 'opened' then f.lines_changed end), 1)                    as avg_lines_changed_opened,
    sum(case when f.action = 'opened' then f.lines_changed end)                              as lines_changed_opened
from {{ ref('fact_pull_requests') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
group by f.date_key, d.full_date, f.repo_language
