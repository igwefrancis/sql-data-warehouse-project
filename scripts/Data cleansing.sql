truncate table silver.crm_cust_info;
select 'Inserting clean data into silver.crm_cust_info ';
# This code do data cleansing for the issues found 
insert into silver.crm_cust_info (
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date)
select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
case	when upper(trim(cst_marital_status)) = 'S' then 'Single'
		when upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'n/a'
end cst_marital_status,
case	when upper(trim(cst_gndr)) = 'F' then 'Female'
		when upper(trim(cst_gndr)) = 'M' then 'Male'
        else 'n/a'
end cst_gndr,
cst_create_date
from (
select 
*,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) t
where flag_last = 1;

truncate table silver.crm_prd_info;
select 'Inserting clean data into silver.crm_prd_info ';
-- data cleansing for product table
insert into silver.crm_prd_info (
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
select
prd_id,
replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
substring(prd_key, 7, length(prd_key)) as prd_key,
prd_nm,
ifnull(prd_cost, 0) as prd_cost,
case	when upper(trim(prd_line)) = 'M' then 'Mountain'
		when upper(trim(prd_line)) = 'R' then 'Road'
        when upper(trim(prd_line)) = 'S' then 'Other Sales'
        when upper(trim(prd_line)) = 'T' then 'Touring'
        else 'n/a'
end prd_line,
prd_start_dt,
date_sub(
	lead(prd_start_dt) over (
    partition by prd_key order by prd_start_dt),
	interval 1 day) as prd_end_dt
from bronze.crm_prd_info;


truncate table silver.crm_sales_details;
select 'Inserting clean data into silver.crm_sales_details';
-- Data cleansing for crm sales table

insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
    )
select
x.sls_ord_num,
x.sls_prd_key,
x.sls_cust_id,
x.sls_order_dt,
x.sls_ship_dt,
x.sls_due_dt,
cast(
case	when x.sls_sales_raw is null or x.sls_sales_raw <= 0 or x.sls_sales_raw <> x.sls_quantity * abs(coalesce(x.sls_price,0))
		then round(x.sls_quantity * abs(x.sls_price), 2)
        else x.sls_sales_raw
end as decimal(12,2)) as sls_sales,
x.sls_quantity,
cast(x.sls_price as decimal(10,2)) as sls_price
from (
select
t.*,
cast(
case	when t.sls_price_raw is null or t.sls_price_raw <= 0
		then round(t.sls_sales_raw / nullif(t.sls_quantity,0), 2)
        else t.sls_price_raw
end as decimal(10,2)) as sls_price
from
(
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,

    CASE WHEN sls_order_dt REGEXP '^[0-9]{8}$'
         THEN STR_TO_DATE(sls_order_dt, '%Y%m%d') END AS sls_order_dt,

    CASE WHEN sls_ship_dt REGEXP '^[0-9]{8}$'
         THEN STR_TO_DATE(sls_ship_dt, '%Y%m%d') END AS sls_ship_dt,

    CASE WHEN sls_due_dt REGEXP '^[0-9]{8}$'
         THEN STR_TO_DATE(sls_due_dt, '%Y%m%d') END AS sls_due_dt,

    CASE 
        WHEN TRIM(sls_quantity) REGEXP '^[0-9]+$'
        THEN CAST(TRIM(sls_quantity) AS UNSIGNED)
    END AS sls_quantity,

    CASE 
        WHEN TRIM(sls_sales) REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
        THEN CAST(TRIM(sls_sales) AS DECIMAL(12,2))
    END AS sls_sales_raw,

    CASE 
        WHEN TRIM(sls_price) REGEXP '^-?[0-9]+(\\.[0-9]+)?$'
        THEN CAST(TRIM(sls_price) AS DECIMAL(10,2))
    END AS sls_price_raw

FROM bronze.crm_sales_details
) t
) x;

truncate table silver.erp_cust_az12;
select 'Inserting clean data into silver.erp_cust_az12 ';
-- Data cleansing for erp cust az12

insert into silver.erp_cust_az12( cid,bdate,gen)
select
case	when cid like 'NAS%' then substring(cid, 4, length(cid))
		else cid
end as cid,
 
CASE
        WHEN parsed_bdate IS NOT NULL
             AND parsed_bdate <= CURDATE()
        THEN parsed_bdate
        ELSE NULL
END AS bdate,

CASE
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE', 'FMALE') THEN 'Female'
        WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE') THEN 'Male'
        ELSE 'N/A'
    END AS gen
FROM (
    SELECT
	cid,
        CASE
            WHEN TRIM(bdate) REGEXP '^[0-9]{8}$'
            THEN STR_TO_DATE(TRIM(bdate), '%Y%m%d')

            WHEN TRIM(bdate) REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
            THEN STR_TO_DATE(TRIM(bdate), '%d/%m/%Y')

            WHEN TRIM(bdate) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(TRIM(bdate), '%Y-%m-%d')

            ELSE NULL
        END AS parsed_bdate,
	gen
    FROM bronze.erp_cust_az12
) t;


truncate table silver.erp_loc_a101;
select 'Inserting clean data into silver.erp_loc_a101 ';
-- Data cleansing for erp loc

insert into silver.erp_loc_a101(cid, cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE
        WHEN clean_cntry = 'DE' THEN 'Germany'
        WHEN clean_cntry IN ('US', 'USA') THEN 'United States'
        WHEN clean_cntry IS NULL OR clean_cntry = '' THEN 'n/a'
        ELSE clean_cntry
    END AS cntry
FROM (
    SELECT
        cid,
        TRIM(
            REPLACE(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''), '\t', '')
        ) AS clean_cntry
    FROM bronze.erp_loc_a101
) t;

-- data claensing for erp mpx cat
-- After quality checks the data looks good so we insert it direct to silver table

truncate table silver.erp_px_cat_g1v2;
select 'Inserting clean data into silver.erp_px_cat_g1v2 ';
insert into silver.erp_px_cat_g1v2 (
id,
cat,
subcat,
maintenance)
select
id,
cat,
subcat,
maintenance
from
bronze.erp_px_cat_g1v2;