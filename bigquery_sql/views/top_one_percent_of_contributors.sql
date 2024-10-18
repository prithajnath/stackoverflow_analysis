with
    cte1 as (
        select
            OwnerUserId as user_id,
            OwnerUserId,
            count(id) as QuestionsAnswered
        from
            `social-computing-436902.stackexchange.stackoverflow_posts`
        where
            PostTypeId = 2
        group by
            1,
            2
    ),
    cte2 as (
        select
            *,
            percent_rank() over (
                order by
                    QuestionsAnswered
            ) as PctRank
        from
            cte1
    )
select
    a.OwnerUserId,
    b.DisplayName,
    b.CreationDate,
    b.Reputation,
    b.AboutMe,
    a.QuestionsAnswered,
    a.PctRank
from
    cte2 a
    join `social-computing-436902.stackexchange.stackoverflow_users` as b on a.user_id = b.Id
where
    PctRank > 0.99;