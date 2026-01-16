/* 
                ****Product Report****

Purpose:
	-This report consolidates key product metrices and behaviours
Highlights:
	1. Gathers essential fields such as product names, category, subcategory and cost
    2. Segments products by revenue to identify high perfomance, mid range or low performance
    3. Aggregates product-level metrices:
		- total orders
        - total sales
        - total quantity sold
        - total customers(unique) 
        - lifespan (in months)
	4. Calculate valuable KPIs:
		- recency (months since last sale)
		- average order revenue (AOR)
        - average monthly revenue
*/
create view product_report as 
with base_query as (
select
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity,
p.product_key,
p.product_name,
p.category,
p.sub_category,
p.cost
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null),

 /*Segments products by revenue to identify high perfomance, mid range or low performance */
product_aggregation as (
select
	product_key,
	product_name,
	category,
	sub_category,
	cost,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	sum(quantity) as total_quantity,
	count(distinct customer_key) as total_customers,
	max(order_date) as last_sales_date,
	timestampdiff(month, min(order_date), max(order_date)) as lifespan,
	round(avg((sales_amount / nullif(quantity, 0))), 2) as avg_selling_price
from base_query
group by 
product_key,
product_name,
category,
sub_category,
cost
)
select
product_key,
product_name,
category,
sub_category,
cost,
last_sales_date,
timestampdiff(month, last_sales_date, current_date()) as recency_in_months,
case	when total_sales > 50000 then 'High-performance'
		when total_sales >= 10000 then 'Mid-Range'
        else 'Low-performer'
end as product_segment,
lifespan,
total_orders,
total_sales,
total_quantity,
total_customers,
avg_selling_price,
case	when total_orders = 0 then 0
		else round((total_sales / total_orders), 2)
end as avg_order_revenue,
case	when lifespan = 0 then total_sales
		else round((total_sales / lifespan), 2)
end as avg_monthly_revenue
from
product_aggregation;