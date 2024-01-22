--stepswise conversion rate 집계
with
event as (
select
  _date,
  user_id,
  '{{ event_name1 }}' as event_name,
  created_at,
from
  event.{{ event_name1 }}

union all

select
  _date,
  user_id,
  '{{ event_name2 }}' as event_name,
  created_at,
from
  event.{{ event_name2 }}
)

, event_augmented as (
select
  _date,
  user_id,
  event_name,
  lead(event_name, 1) over (partition by user_id order by created_at) as event_name_next,
from
  event
)
, aggregated_by_date_and_user as (
select
  _date,
  user_id,
  logical_or(event_name_next = '{{ event_name2 }}') as is_converted,
from
  event_augmented
where
  event_name = '{{ event_name1 }}'
  and _date between '{{ date1 }}' and '{{ date2 }}'
group by
  _date, user_id
)
, aggregated_by_date as (
select
  _date,
  countif(is_converted) / count(1) as value,
from
  aggregated_by_date_and_user
group by
  _date
)
select
  '{{ event_name1 }}' as event_name1,
  '{{ event_name2 }}' as event_name2,
  array(
    select as struct
      _date as date,
      value,
    from
      aggregated_by_date
    order by
      date
  ) as data,
