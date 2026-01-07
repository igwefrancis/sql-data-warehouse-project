/*
This script loads csv files into our database
and If your column allows NULLs, the script tell MySQL to treat empty strings as NULL during import
*/
 -- ========== CRM CUSTOMER INFO ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_info.csv'
	INTO TABLE bronze.crm_cust_info
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES
	(@cst_id, @cst_key, @cst_firstname, @cst_lastname, @cst_marital_status, @cst_gndr, @cst_create_date)
	SET
	cst_id = NULLIF(@cst_id, ''),
	cst_key = NULLIF(@cst_key, ''),
	cst_firstname = @cst_firstname,
	cst_lastname = @cst_lastname,
	cst_marital_status = @cst_marital_status,
	cst_gndr = @cst_gndr,
	cst_create_date = @cst_create_date;


ALTER TABLE bronze.crm_prd_info
MODIFY prd_id        int,
MODIFY prd_key       VARCHAR(50),
MODIFY prd_nm        VARCHAR(255),
MODIFY prd_cost      VARCHAR(50),
MODIFY prd_line      VARCHAR(50),
MODIFY prd_start_dt  VARCHAR(50),
MODIFY prd_end_dt    VARCHAR(50);

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(
  prd_id,
  prd_key,
  prd_nm,
  prd_cost,
  prd_line,
  prd_start_dt,
  prd_end_dt
);

select
* from bronze.crm_prd_info;

UPDATE bronze.crm_prd_info
SET prd_cost = NULL
WHERE TRIM(prd_cost) = '';

UPDATE bronze.crm_prd_info
SET prd_start_dt = NULL
WHERE TRIM(prd_start_dt) = '';

UPDATE bronze.crm_prd_info
SET prd_cost = CAST(prd_cost AS UNSIGNED)
WHERE prd_cost IS NOT NULL;

UPDATE bronze.crm_prd_info
SET prd_start_dt = STR_TO_DATE(prd_start_dt, '%d/%m/%Y')
WHERE prd_start_dt IS NOT NULL;

ALTER TABLE bronze.crm_prd_info
MODIFY prd_cost INT NULL,
MODIFY prd_start_dt DATE NULL;

UPDATE bronze.crm_prd_info
SET prd_end_dt = NULL
WHERE TRIM(prd_start_dt) = '';

UPDATE bronze.crm_prd_info
SET prd_end_dt = NULL
WHERE TRIM(prd_end_dt) = '';

UPDATE bronze.crm_prd_info
SET prd_end_dt = NULL
WHERE prd_end_dt IS NOT NULL
  AND TRIM(prd_end_dt) = '';

UPDATE bronze.crm_prd_info
SET prd_end_dt =
    STR_TO_DATE(
        NULLIF(TRIM(prd_end_dt), ''),
        '%d/%m/%Y'
    )
WHERE prd_end_dt REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$';


ALTER TABLE bronze.crm_prd_info
MODIFY prd_end_dt DATE NULL;


select
* from bronze.crm_prd_info;


 -- ========== CRM PRODUCT INFO ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
	INTO TABLE bronze.crm_prd_info
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n'
	IGNORE 1 LINES
	(@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
	SET 
		prd_id = NULLIF(@prd_id, ''),
		prd_key = NULLIF(@prd_key, ''),
		prd_nm = NULLIF(@prd_nm, ''),
		prd_cost = NULLIF(@prd_cost, ''),
		prd_line = NULLIF(@prd_line, ''),
		prd_start_dt = NULLIF(@prd_start_dt, ''),
		prd_end_dt = NULLIF(@prd_end_dt, '');


    -- ========== CRM SALES DETAILS ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
	INTO TABLE bronze.crm_sales_details
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n'
	IGNORE 1 LINES
	(@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
	SET
		sls_ord_num   = NULLIF(TRIM(@sls_ord_num), ''),
		sls_prd_key   = NULLIF(TRIM(@sls_prd_key), ''),
		sls_cust_id   = NULLIF(TRIM(@sls_cust_id), ''),
		sls_order_dt  = NULLIF(TRIM(@sls_order_dt), ''),   -- load as string first
		sls_ship_dt   = NULLIF(TRIM(@sls_ship_dt), ''),
		sls_due_dt    = NULLIF(TRIM(@sls_due_dt), ''),
		sls_sales     = NULLIF(TRIM(@sls_sales), ''),
		sls_quantity  = NULLIF(TRIM(@sls_quantity), ''),
		sls_price     = NULLIF(TRIM(@sls_price), '');
        
ALTER TABLE bronze.crm_sales_details
MODIFY sls_sales    VARCHAR(50),
MODIFY sls_quantity VARCHAR(50),
MODIFY sls_price    VARCHAR(50);


  -- ========== ERP CUSTOMER AZ12 ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_az12.csv'
	INTO TABLE bronze.erp_cust_az12
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;

 -- ========== ERP LOCATION A101 ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/loc_a101.csv'
	INTO TABLE bronze.erp_loc_a101
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;

 -- ========== ERP PX CATEGORY ==========
	LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/px_cat_g1v2.csv'
	INTO TABLE bronze.erp_px_cat_g1v2
	FIELDS TERMINATED BY ',' 
	LINES TERMINATED BY '\n' 
	IGNORE 1 LINES;
    
