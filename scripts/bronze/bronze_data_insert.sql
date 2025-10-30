/*Script Purpose:
    This loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from csv Files to bronze tables.
Note: pgAdmin doesn't support COPY inside stored procedures. 
So to automate the multiple table load process using this SQL script that you run from pgAdmin's query tool.*/

-- Start total timer
SELECT clock_timestamp() INTO TEMP temp_timer;
DO $$
BEGIN
    RAISE NOTICE 'Loading all tables in bronze layer';
END $$;	
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.crm_cust_info...';
    start_time := clock_timestamp();
	
      -- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.crm_cust_info;
		-- Step 2: Load fresh data from CSV
		COPY bronze.crm_cust_info
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_crm\\cust_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.crm_cust_info in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.crm_prd_info...';
    start_time := clock_timestamp();
		-- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.crm_prd_info;
		-- Step 2: Load fresh data from CSV
		COPY bronze.crm_prd_info
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_crm\\prd_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.crm_prd_info in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.crm_sales_details...';
    start_time := clock_timestamp();
		
		-- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.crm_sales_details;
		-- Step 2: Load fresh data from CSV
		COPY bronze.crm_sales_details
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_crm\\sales_details.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.crm_sales_details in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.erp_loc_a101...';
    start_time := clock_timestamp();
		-- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.erp_loc_a101;
		-- Step 2: Load fresh data from CSV
		COPY bronze.erp_loc_a101
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_erp\\loc_a101.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.erp_loc_a101 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.erp_cust_az12...';
    start_time := clock_timestamp();
		-- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.erp_cust_az12;
		-- Step 2: Load fresh data from CSV
		COPY bronze.erp_cust_az12
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_erp\\cust_az12.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.erp_cust_az12 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;

DO $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Loading bronze.erp_px_cat_g1v2...';
    start_time := clock_timestamp();
		-- Step 1: Remove all existing rows
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		-- Step 2: Load fresh data from CSV
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\\Program Files\\PostgreSQL\\18\\data\\datawarehouse_project_raw_data\\source_erp\\px_cat_g1v2.csv'
		WITH (
		    FORMAT csv,
		    HEADER true
		);
   GET DIAGNOSTICS rows_loaded = ROW_COUNT;
    end_time := clock_timestamp();

    RAISE NOTICE 'Loaded % rows into bronze.erp_px_cat_g1v2 in % seconds.',
        rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
END $$;
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

/*
Output:
NOTICE:  Loading all tables in bronze layer
NOTICE:  Loading bronze.crm_cust_info...
NOTICE:  Loaded 18494 rows into bronze.crm_cust_info in 0.102311 seconds.
NOTICE:  Loading bronze.crm_prd_info...
NOTICE:  Loaded 397 rows into bronze.crm_prd_info in 0.007559 seconds.
NOTICE:  Loading bronze.crm_sales_details...
NOTICE:  Loaded 60398 rows into bronze.crm_sales_details in 0.154664 seconds.
NOTICE:  Loading bronze.erp_loc_a101...
NOTICE:  Loaded 18484 rows into bronze.erp_loc_a101 in 0.021631 seconds.
NOTICE:  Loading bronze.erp_cust_az12...
NOTICE:  Loaded 18484 rows into bronze.erp_cust_az12 in 0.112480 seconds.
NOTICE:  Loading bronze.erp_px_cat_g1v2...
NOTICE:  Loaded 37 rows into bronze.erp_px_cat_g1v2 in 0.002168 seconds.
NOTICE:  Total load time: 0.405137 seconds.
DO
Query returned successfully in 481 msec.
*/
