/*
Question 5. Number of rows (yellow, March 2021) (1 point) 
*/

select count(1) rc from yellow_tripdata
where filename = 'yellow_tripdata_2021-03.csv';

/*
1925152
*/