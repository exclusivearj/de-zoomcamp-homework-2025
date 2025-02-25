### Quarter 5: Taxi Quarterly Revenue Growth

1. Create a new model `fct_taxi_trips_quarterly_revenue.sql`
2. Compute the Quarterly Revenues for each year for based on `total_amount`
3. Compute the Quarterly YoY (Year-over-Year) revenue growth 
  * e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1
  * e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4

Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow

- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q2, worst: 2020/Q1}, yellow: {best: 2020/Q3, worst: 2020/Q4}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q2, worst: 2020/Q1}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}
- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q3, worst: 2020/Q4}


# Answer:
```sql
-- Query 2: Compute the Quarterly YoY (Year-over-Year) revenue growth
WITH quarterly_revenue AS (
  SELECT 
    service_type,
    year,
    quarter,
    CONCAT(year, '/Q', quarter) AS year_quarter,
    SUM(revenue_quarterly_total_amount) AS quarterly_revenue
  FROM 
    `trips_data_all.fct_taxi_trips_quarterly_revenue`
  GROUP BY 
    service_type, year, quarter, year_quarter
)

SELECT 
  current_year.service_type,
  current_year.year_quarter,
  current_year.quarterly_revenue AS current_revenue,
  previous_year.quarterly_revenue AS previous_revenue,
  CASE 
    WHEN previous_year.quarterly_revenue = 0 THEN NULL
    ELSE ROUND((current_year.quarterly_revenue - previous_year.quarterly_revenue) 
      / previous_year.quarterly_revenue * 100, 2)
  END AS yoy_growth_percentage
FROM 
  quarterly_revenue current_year
LEFT JOIN 
  quarterly_revenue previous_year
  ON current_year.service_type = previous_year.service_type
  AND current_year.quarter = previous_year.quarter
  AND current_year.year = previous_year.year + 1
WHERE 
  current_year.year > (SELECT MIN(year) FROM quarterly_revenue)
ORDER BY 
  current_year.service_type, 
  current_year.year, 
  current_year.quarter
```

- green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}