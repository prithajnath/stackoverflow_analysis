select
    OwnerUserId,
    count(Id) as num_questions_answered
from
    `social-computing-436902.stackexchange.stackoverflow_posts`
where
    PostTypeId = 2
group by
    1;