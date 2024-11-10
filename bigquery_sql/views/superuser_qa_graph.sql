select
    a.OwnerUserId as answer_user_id,
    q.OwnerUserId as question_user_id
from
    `social-computing-436902.stackexchange.stackoverflow_posts` a
    join `social-computing-436902.stackexchange.stackoverflow_posts` q on a.ParentId = q.Id
where
    a.OwnerUserId in (
        select
            user_id
        from
            `social-computing-436902.stackexchange.top_one_percent_of_contributors`
    );