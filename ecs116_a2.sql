set search_path to public

--step3a
alter table reviews 
add column datetime timestamp;

UPDATE reviews
SET datetime = to_timestamp((TO_CHAR(date::date, 'YYYY-MM-DD') || ' 12:00:00'), 'YYYY-MM-DD hh24:mi:ss');

with yrs as 
(
select datetime, cast(date_part('Year', datetime) as varchar) as yr
from reviews
)
select count(datetime), yr
from yrs
group by yr
order by yr

--step3b
alter table reviews 
add column comments_tsv tsvector;

update reviews 
set comments_tsv = to_tsvector(comments);

create index if not exists comments_tsv_in_reviews
on reviews using GIN(comments_tsv);

--with index
select count(*)
from reviews r
where comments_tsv @@ to_tsquery('awesome')
and datetime >='2023-01-01'
and datetime <='2023-12-31';

--without index
select count(*)
from reviews r
where comments ilike '%awesome%'
and datetime >='2023-01-01';
and datetime <='2023-12-31'; 

select left(to_char(date, 'YYYY-MM-DD'),4) as year, count(*)
from reviews r
where comments_tsv @@ to_tsquery('horrible')
group by year 
order by year