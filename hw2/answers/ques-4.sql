/*
Question 4. Number of rows (green, 2020) (1 point) 
*/

with data as (
select filename, count(1) rc from green_tripdata
where filename like 'green_tripdata_2020%'
group by 1
)

select sum(rc) as total_rows
from data;

/*
"total_rows"
1,734,051
*/