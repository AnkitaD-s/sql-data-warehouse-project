-- PROCEDURE: silver.load_silver()

-- DROP PROCEDURE IF EXISTS silver.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver(
	)
LANGUAGE 'sql'
AS $BODY$

/*Script Purpose:
    This loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `INSERT` command to load data from bronze table to silver tables.
	
*/
-- Start total timer
SELECT clock_timestamp() INTO TEMP temp_timer;
DO $$
BEGIN
    RAISE NOTICE 'Loading all tables in silver layer';
END $$;	
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.crm_cust_info...';
    start_time := clock_timestamp();
	
-- Step 1: Remove all existing rows	
truncate table silver.crm_cust_info;
-- Step 2: Load fresh data from bronze layer
insert into silver.crm_cust_info 
(
select cst_id,
cst_key,
Trim(cst_firstname),
Trim(cst_lastname),
case 
when Upper(Trim(cst_marital_status)) = 's' then 'Single'
when Upper(Trim(cst_marital_status)) = 'M' then 'Married'
else 'n/a'
end as cst_marital_status,
case
when Upper(Trim(cst_gndr)) = 'F' then 'Female'
when Upper(Trim(cst_gndr)) = 'M' then 'Male'
else 'n/a'
end as cst_gndr,
cst_create_date
from (
select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag from bronze.crm_cust_info
) where flag = 1
);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.crm_cust_info in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
--------------------------------------------------------------------------------
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.crm_prd_info...';
    start_time := clock_timestamp();
-- Step 1: Remove all existing rows
truncate table silver.crm_prd_info;
-- Step 2: Load fresh data from bronze layer
insert into silver.crm_prd_info
(
select prd_id,
replace(substring(prd_key from 1 for 5),'-','_') cat_id,
substring(prd_key from 7 for length(prd_key)) prd_key,
prd_nm,
coalesce(prd_cost,0) prd_cost,
case UPPER(TRIM(prd_line))
when 'M' then 'Mountain'
when 'T' then 'Touring'
when 'R' then 'Road'
when 'S' then 'Others'
else 'n/a'
end as prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast((lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)) - interval '1  day' as date) as prd_end_dt
from bronze.crm_prd_info
order by prd_id
);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.crm_prd_info in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
-------------------------------------------------------------------------------- 
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.crm_sales_details...';
    start_time := clock_timestamp();
-- Step 1: Remove all existing rows
truncate table silver.crm_sales_details;
-- Step 2: Load fresh data from bronze layer
insert into silver.crm_sales_details (
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = '0' or length(cast(sls_order_dt as varchar)) < 8 then null
	 else to_date(sls_order_dt::text, 'YYYYMMDD')
end as sls_order_dt,
case when sls_ship_dt = '0' or length(cast(sls_ship_dt as varchar)) < 8 then null
	 else to_date(sls_ship_dt::text, 'YYYYMMDD')
end as sls_ship_dt,
case when sls_due_dt = '0' or length(cast(sls_due_dt as varchar)) < 8 then null
	 else to_date(sls_due_dt::text, 'YYYYMMDD')
end as sls_due_dt,
case when sls_sales <=0 or sls_sales is null or sls_sales != sls_quantity*abs(sls_price)then sls_quantity * abs(sls_price)
	 else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price =0 or sls_price is null then sls_sales/nullif(sls_quantity,0)
	 else abs(sls_price)
end as sls_price
from bronze.crm_sales_details);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.crm_sales_details in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
--------------------------------------------------------------------------------
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.erp_cust_az12...';
    start_time := clock_timestamp();
-- Step 1: Remove all existing rows
truncate table silver.erp_cust_az12;
-- Step 2: Load fresh data from bronze layer
insert into silver.erp_cust_az12
(
select
case when cid like 'NAS%' then substring(cid, 4, length(cid))
	 else cid 
end as cid,
case when bdate > current_date then null
	 else bdate 
end as bdate,
case when Upper(Trim(gen)) in ('M','MALE') then 'Male'
	 when Upper(Trim(gen)) in ('F','FEMALE') then 'Female'
	 else 'n/a'
end as gen
from bronze.erp_cust_az12
);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.erp_cust_az12 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
--------------------------------------------------------------------------------
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.erp_loc_a101...';
    start_time := clock_timestamp();
-- Step 1: Remove all existing rows
truncate table silver.erp_loc_a101;
-- Step 2: Load fresh data from bronze layer
insert into silver.erp_loc_a101
(
select replace (cid,'-','') as cid, 
case when Upper(Trim(cntry)) = 'DE' then 'Germany'
	 when Upper(Trim(cntry)) in ('US','USA') then 'United States'
	 when cntry is null or Trim(cntry) ='' then 'n/a'
	 else cntry
end as cntry
from bronze.erp_loc_a101
);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.erp_loc_a101 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
--------------------------------------------------------------------------------
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading silver.erp_px_cat_g1v2...';
    start_time := clock_timestamp();
-- Step 1: Remove all existing rows
truncate table silver.erp_px_cat_g1v2;
-- Step 2: Load fresh data from bronze layer
insert into silver.erp_px_cat_g1v2
(
select * from bronze.erp_px_cat_g1v2
);
GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into silver.erp_px_cat_g1v2 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
--------------------------------------------------------------------------------
-- End total timer and load duration
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP := clock_timestamp();
BEGIN
    SELECT * INTO start_time FROM temp_timer;
    RAISE NOTICE 'Total load time: % seconds.', EXTRACT(EPOCH FROM end_time - start_time);
    DROP TABLE temp_timer;
END $$;


$BODY$;
ALTER PROCEDURE silver.load_silver()
    OWNER TO postgres;

--------------------------------------------------------------------------------
/*
OUTPUT:
NOTICE:  Loading all tables in silver layer
NOTICE:  Loading silver.crm_cust_info...
NOTICE:  Loaded 18485 rows into silver.crm_cust_info in 0.143281 seconds.
NOTICE:  Loading silver.crm_prd_info...
NOTICE:  Loaded 397 rows into silver.crm_prd_info in 0.009743 seconds.
NOTICE:  Loading silver.crm_sales_details...
NOTICE:  Loaded 60398 rows into silver.crm_sales_details in 1.362339 seconds.
NOTICE:  Loading silver.erp_cust_az12...
NOTICE:  Loaded 18484 rows into silver.erp_cust_az12 in 0.076044 seconds.
NOTICE:  Loading silver.erp_loc_a101...
NOTICE:  Loaded 18484 rows into silver.erp_loc_a101 in 0.298018 seconds.
NOTICE:  Loading silver.erp_px_cat_g1v2...
NOTICE:  Loaded 37 rows into silver.erp_px_cat_g1v2 in 0.002130 seconds.
NOTICE:  Total load time: 1.899508 seconds.
CALL

Query returned successfully in 1 secs 975 msec.
*/


