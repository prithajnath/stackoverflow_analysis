with
    cte1 as (
        select
            OwnerUserId,
            count(id) as QuestionsAnswered
        from
            `social-computing-436902.stackexchange.stackoverflow_posts`
        where
            PostTypeId = 2
        group by
            1
    ),
    cte2 as (
        select
            *,
            ntile (100) over (
                order by
                    QuestionsAnswered asc
            ) as Pctile
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
    a.Pctile
from
    cte2 a
    join `social-computing-436902.stackexchange.stackoverflow_users` as b on a.OwnerUserId = b.Id
where
    Pctile >= 99;