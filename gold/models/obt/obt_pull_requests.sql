{{ config(enabled=var('build_obt', false)) }}
-- optional one big table: every pull request event with its dimensions flattened in.
-- Off by default; build it with: dbt build --vars '{build_obt: true}'
select
    f.event_id, f.pr_id, f.pr_number, f.action, f.pr_state, f.is_merged, f.is_draft,
    f.pr_size, f.additions, f.deletions, f.lines_changed, f.changed_files, f.commit_count,
    f.merge_time_hours, f.created_at, f.pr_created_at, f.merged_at, f.closed_at,
    d.full_date, d.year, d.month, d.month_name, d.day_name, d.is_weekend,
    t.hour_key, t.day_part,
    r.repo_id, r.repo_name, r.repo_owner, r.primary_language,
    a.actor_id, a.actor_login, a.actor_type,
    f.pr_author, f.pr_author_type, f.pr_author_is_bot, f.author_association, f.base_branch
from {{ ref('fact_pull_requests') }} f
join {{ ref('dim_date') }} d on d.date_key = f.date_key
join {{ ref('dim_time') }} t on t.hour_key = f.hour_key
join {{ ref('dim_repo') }} r on r.repo_id = f.repo_id
join {{ ref('dim_actor') }} a on a.actor_id = f.actor_id
