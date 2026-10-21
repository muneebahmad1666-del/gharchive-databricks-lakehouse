-- every GitHub event type with a business category; -1 catches anything unexpected
select
    cast(event_type_key as int) as event_type_key,
    event_type,
    event_category
from values
    (-1, 'Unknown',                       'unknown'),
    (1,  'CommitCommentEvent',            'collaboration'),
    (2,  'CreateEvent',                   'code'),
    (3,  'DeleteEvent',                   'code'),
    (4,  'ForkEvent',                     'social'),
    (5,  'GollumEvent',                   'repository_admin'),
    (6,  'IssueCommentEvent',             'collaboration'),
    (7,  'IssuesEvent',                   'collaboration'),
    (8,  'MemberEvent',                   'repository_admin'),
    (9,  'PublicEvent',                   'repository_admin'),
    (10, 'PullRequestEvent',              'collaboration'),
    (11, 'PullRequestReviewCommentEvent', 'collaboration'),
    (12, 'PullRequestReviewEvent',        'collaboration'),
    (13, 'PushEvent',                     'code'),
    (14, 'ReleaseEvent',                  'code'),
    (15, 'WatchEvent',                    'social')
    as t(event_type_key, event_type, event_category)
