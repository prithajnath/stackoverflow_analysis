select
  a.Id as user_id,
  year,
  month,
  coalesce(num_questions_answered, 0) as num_questions_answered,
from
  `social-computing-436902.stackexchange.stackoverflow_users` a
  cross join (
    select
      date_trunc (
        date_add (date '2008-01-01', interval x month),
        month
      ) as year_month
    from
      unnest (generate_array (0, 12 * (2024 - 2008) + 11)) as x
  )
  left join (
    select
      OwnerUserId,
      extract(
        year
        from
          CreationDate
      ) as year,
      extract(
        month
        from
          CreationDate
      ) as month,
      count(Id) as num_questions_answered
    from
      `social-computing-436902.stackexchange.stackoverflow_posts`
    where
      PostTypeId = 2
    group by
      1,
      2,
      3
  ) b on a.Id = b.OwnerUserId