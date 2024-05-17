--step3a
alter table reviews 
add column datetime timestamp;

UPDATE reviews
SET datetime = to_timestamp((TO_CHAR(date::date, 'YYYY-MM-DD') || ' 12:00:00'), 'YYYY-MM-DD hh24:mi:ss');

alter table reviews 
add column comments_tsv tsvector;

--step3b
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