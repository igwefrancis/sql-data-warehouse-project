/* 
                ****Customer Report****

Purpose:
	-This report consolidates key customer metrices and behaviours
Highlights:
	1. Gathers essential fields such as names, ages, and transaction details
    2. Segments customers into categories(VIP,Regular, New) and age group
    3. Aggregates customer-level metrices:
		- total orders
        - total sales
        - total quantity purchased
        - total products 
        - lifespan (in months)
	4. Calculate valuable KPIs:
		- recency (months since last order)
		- average order values
        - average monthly spend
*/
-- BAse query: retrives core colums from tables
create view customer_report as 
with base_query as (
/* 
Solution
1. Base Query: Retrieves core columns from tables
some transformation is done here
*/

select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
concat(c.first_name, ' ', c.last_name) as customer_name,
TIMESTAMPDIFF(YEAR, c.birthdate, CURDATE()) AS age
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key = f.customer_key
where order_date is not null),

customer_aggregation as (
 /*segment customers into categories (vip, regular and new) */
select 
customer_key,
customer_number,
customer_name,
age,
count(distinct order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(distinct product_key) as total_products,
max(order_date) as last_order_date,
timestampdiff(month, min(order_date), max(order_date)) as lifespan
from base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age
)
   select 
   customer_key,
	customer_number,
	customer_name,
	age,
	case	when age < 20 then 'Under 20'
			when age between 20 and 29 then '20-29'
			when age between 30 and 39 then '30-39'
			when age between 40 and 49 then '40-49'
			else '50 and above'
	end as age_group,
	case	when lifespan >= 12 and total_sales > 5000 then 'VIP'
			when  lifespan >= 12 and total_sales <= 5000 then 'Regular'
			else 'New'
	end as customer_segment,
    total_orders,
    total_sales,
    total_quantity,
    total_products,
    last_order_date,
	TIMESTAMPDIFF(year, last_order_date, CURDATE()) AS recency,
    lifespan,
    -- compute average order value
    case	when total_orders = 0 then 0
			else total_sales / total_orders
	end  as avg_order_value,
    -- computing average monthly spend
     case	when lifespan = 0 then total_sales
			else round((total_sales / lifespan),0)
	end  as avg_monthly_spend
    from customer_aggregation;

select
* from gold.customer_report