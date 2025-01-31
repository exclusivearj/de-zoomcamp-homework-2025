/*
Question 3. Number of rows (yellow, 2020) (1 point)
*/

with data as (
select filename, count(1) rc from yellow_tripdata
where filename like 'yellow_tripdata_2020%'
group by 1
)

select sum(rc) as total_rows
from data;

/*
"total_rows"
11,944,137

6,299,354
6405008

*/