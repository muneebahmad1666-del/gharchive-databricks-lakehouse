# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
BASE_REQUIRED = ["event_id", "created_at", "actor_id", "repo_id"]

REQUIRED = {
    "events":              BASE_REQUIRED + ["type", "repo_name"],
    "create_events":       BASE_REQUIRED + ["ref_type"],
    "pull_request_events": BASE_REQUIRED + ["pr_id", "pr_number", "action", "pr_state",
                                            "pr_created_at", "base_branch"],
    "push_events":         BASE_REQUIRED + ["push_id", "ref"],
    "issue_events":        BASE_REQUIRED + ["issue_id", "issue_number", "issue_state", "action"],
    "fork_events":         BASE_REQUIRED + ["forkee_id", "forkee_name"],
    "watch_events":        BASE_REQUIRED + ["action"],
}

# optional columns: a null is fine, but a default makes gold queries simpler
DEFAULTS = {
    "pull_request_events": {"repo_language": "unknown"},
    "create_events":       {"master_branch": "unknown", "pusher_type": "unknown"},
}
KNOWN_TYPES = ("PushEvent", "PullRequestEvent", "IssuesEvent", "IssueCommentEvent", "WatchEvent",
               "ForkEvent", "CreateEvent", "DeleteEvent", "PullRequestReviewEvent",
               "PullRequestReviewCommentEvent", "ReleaseEvent", "CommitCommentEvent",
               "MemberEvent", "GollumEvent", "PublicEvent")

# (rule name, technique, severity, SQL predicate that is TRUE for a BAD row)
COMMON_RULES = [
    ("ids_positive",          "validation",  "error", "event_id <= 0 OR actor_id <= 0 OR repo_id <= 0"),
    ("event_hour_range",      "validation",  "error", "event_hour < 0 OR event_hour > 23"),
    ("created_at_range",      "validation",  "error", "created_at < '2011-01-01' OR created_at > current_timestamp()"),
    ("event_date_consistent", "enrichment",  "error", "event_date <> to_date(created_at)"),
]
TABLE_RULES = {
    "events": [
        ("repo_name_format", "validation",      "error", "NOT repo_name RLIKE '^[^/]+/[^/]+$'"),
        ("type_is_known",    "filtering",       "warn",  f"type NOT IN {KNOWN_TYPES}"),
        ("login_trimmed",    "standardization", "warn",  "actor_login <> trim(actor_login)"),
    ],
    "create_events": [
        ("ref_type_valid", "validation", "warn", "ref_type NOT IN ('repository', 'branch', 'tag')"),
    ],
    "pull_request_events": [
        ("counts_not_negative",   "validation",    "error", "additions < 0 OR deletions < 0 OR changed_files < 0 OR commit_count < 0"),
        ("state_is_valid",        "validation",    "error", "pr_state NOT IN ('open', 'closed')"),
        ("merged_has_timestamp",  "business_rule", "error", "is_merged AND merged_at IS NULL"),
        ("merge_after_creation",  "business_rule", "error", "merged_at < pr_created_at"),
        ("lines_changed_correct", "enrichment",    "error", "lines_changed <> additions + deletions"),
        ("pr_size_outlier",       "cleansing",     "warn",  "lines_changed > 100000"),
    ],
    "push_events": [
        ("push_id_positive",    "validation", "error", "push_id <= 0"),
        ("counts_not_negative", "validation", "error", "commit_count < 0 OR distinct_commit_count < 0"),
        ("distinct_le_total",   "validation", "warn",  "distinct_commit_count > commit_count"),
    ],
    "issue_events": [
        ("issue_ids_positive", "validation", "error", "issue_id <= 0 OR issue_number <= 0"),
        ("comments_not_negative", "validation", "error", "comment_count < 0"),
        ("issue_state_valid",  "validation", "error", "issue_state NOT IN ('open', 'closed')"),
    ],
    "fork_events": [
        ("forkee_id_positive", "validation", "error", "forkee_id <= 0"),
        ("forkee_name_format", "validation", "error", "NOT forkee_name RLIKE '^[^/]+/[^/]+$'"),
    ],
    "watch_events": [
        ("action_is_started",  "business_rule", "warn",  "action <> 'started'"),
        ("is_star_consistent", "enrichment",    "error", "is_star <> (action = 'started')"),
    ],
}
# columns to lowercase: GitHub logins are case-insensitive, and these are fixed vocabularies
# (ref, master_branch, repo_name, and forkee_name are left alone on purpose)
LOWERCASE = {
    "events":              ["actor_login", "org_login"],
    "create_events":       ["ref_type", "pusher_type"],
    "pull_request_events": ["action", "pr_state", "pr_author"],
    "issue_events":        ["action", "issue_state"],
    "watch_events":        ["action"],
}