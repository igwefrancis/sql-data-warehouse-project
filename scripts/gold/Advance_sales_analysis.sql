-- Yealy performance 
select
year(order_date) as Order_year,
sum(sales_amount) as Total_sales,
count(distinct customer_key) as Total_customers,
sum(quantity) as Total_quantity
from gold.fact_sales
where order_date is not null
group by year(order_date)
order by year(order_date);

-- Monthly performance
SELECT
    DATE_FORMAT(order_date, '%Y-%m-01') AS Order_date,
    SUM(sales_amount) AS total_sales,
    COUNT(DISTINCT customer_key) AS total_customers,
    SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY  DATE_FORMAT(order_date, '%Y-%m-01')
ORDER BY DATE_FORMAT(order_date, '%Y-%m-01');

-- Cummulative Analysis
-- calculate the total sales per month
-- and the running total of sales and moving average per month over time

select
Order_date,
total_sales,
sum(total_sales) over (partition by order_date order by Order_date) as Running_total_sales,
avg(Avg_price) over (partition by order_date order by order_date) as Moving_average_price
from (
SELECT
    date_format(order_date, '%Y-%m-01') AS Order_date,
    SUM(sales_amount) AS total_sales,
    avg(sales_amount) as Avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY  DATE_FORMAT(order_date, '%Y-%m-01')
)t;

-- calculate the total sales per month
-- and the running total of sales and moving average per year over time
select
Order_date,
total_sales,
sum(total_sales) over (order by Order_date) as Running_total_sales,
avg(Avg_price) over (order by order_date) as Moving_average_price
from (
SELECT
    year(order_date) AS Order_date,
    SUM(sales_amount) AS total_sales,
    avg(sales_amount) as Avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY  year(order_date)
)t;

-- Analyze yearly performance of products by comparing each products sales to both its average sales performance
-- and the previous years sales

with yearly_product_sales as (
select
year(f.order_date) as order_year,
p.product_name,
sum(f.sales_amount) as current_sales
from
gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where f.order_date is not null
group by year(f.order_date), p.product_name
)
select
order_year,
product_name,
current_sales,
avg(current_sales) over (partition by product_name) as avg_sales,
current_sales - avg(current_sales) over (partition by product_name) as diff_avg,
case	when current_sales - avg(current_sales) over (partition by product_name) > 0 then 'Above_avg'
		when current_sales - avg(current_sales) over (partition by product_name) < 0 then 'Below_avg'
        else 'Avg'
end avg_change,
lag(current_sales) over (partition by product_name order by order_year) as py_year,
current_sales - lag(current_sales) over (partition by product_name order by order_year) as diff_py,
case	when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
		when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
        else 'No_change'
end py_change
from yearly_product_sales
order by product_name, order_year;

-- Part to whole analysis
-- which categoried contributed the most to overall sales
with category_sales as (
select
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on p.product_key = f.product_key
group by category)

select 
category,
total_sales,
sum(total_sales) over () as overall_sales,
concat(round((total_sales / sum(total_sales) over ()) * 100, 2), '%') as percentage_of_total
from category_sales
order by total_sales desc;


with country_sales as (
select
country,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
group by country)

select 
country,
total_sales,
sum(total_sales) over () as overall_sales,
concat(round((total_sales / sum(total_sales) over ()) * 100, 2), '%') as percentage_of_total
from country_sales
order by total_sales desc;


-- Data segmentation
-- segments products into cost ranges and count how many products fall into each segment
with product_segments as (
select
product_key,
product_name,
cost,
case	when cost < 100 then 'Below 100'
		when cost between 100 and 500 then '100-500'
        when cost between 500 and 1000 then '500-1000'
        else 'Above 1000'
end cost_range
from gold.dim_products)

select
cost_range,
count(product_key) as total_products
from product_segments
group by cost_range
order by total_products desc;

/* Group customers into three segements based on their spending behaviour 
	-VIP: customers with at least 12 months of history and spending more than 5,000.
    -Regular: customers with at least 12 months of history but spending 5,000 or less.
    -New: customers with a lifespan less than 12 months.
and find the total number of the customer by each group*/

with customer_spending as (
select
c.customer_key,
sum(f.sales_amount) as total_spending,
min(order_date) as first_order,
max(order_date) as last_order,
timestampdiff(month, min(order_date), max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c 
on f.customer_key = c.customer_key
group by c.customer_key
)
select
customer_segment,
count(customer_key) as total_customers
from (
select
customer_key,
case	when lifespan > 12 and total_spending > 5000 then 'VIP'
		when lifespan >= 12 and total_spending <= 5000 then 'Regular'
        else 'New'
end customer_segment
from customer_spending) t
group by customer_segment
order by total_customers desc;
