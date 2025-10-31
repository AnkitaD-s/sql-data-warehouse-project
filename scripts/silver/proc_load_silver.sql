/*Script Purpose:
    This loads data into the 'silver' schema from external CSV files. 
    It performs the following actions:
    - Truncates the silver tables before loading data.
    - Uses the `INSERT` command to load data from bronze table to silver tables.
*/
-- PROCEDURE: silver.load_silver()
-- DROP PROCEDURE IF EXISTS silver.load_silver();

CREATE OR REPLACE PROCEDURE silver.load_silver(
	)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    total_start TIMESTAMP := clock_timestamp();
    total_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    rows_loaded INTEGER;
BEGIN
    RAISE NOTICE 'Starting silver layer load...';

    -- crm_cust_info----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.crm_cust_info...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_cust_info;

        INSERT INTO silver.crm_cust_info
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
            FROM bronze.crm_cust_info
        ) sub
        WHERE flag = 1;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into crm_cust_info in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading crm_cust_info: %', SQLERRM;
    END;
    -- crm_prd_info----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.crm_prd_info...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_prd_info;

        INSERT INTO silver.crm_prd_info
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key FROM 7) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'T' THEN 'Touring'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Others'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE),
            CAST((LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) - INTERVAL '1 day' AS DATE)
        FROM bronze.crm_prd_info
        ORDER BY prd_id;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into crm_prd_info in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- crm_sales_details----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.crm_sales_details...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.crm_sales_details;

        INSERT INTO silver.crm_sales_details
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = '0' OR LENGTH(sls_order_dt::TEXT) < 8 THEN NULL ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') END,
            CASE WHEN sls_ship_dt = '0' OR LENGTH(sls_ship_dt::TEXT) < 8 THEN NULL ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') END,
            CASE WHEN sls_due_dt = '0' OR LENGTH(sls_due_dt::TEXT) < 8 THEN NULL ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') END,
            CASE WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price)
                 THEN sls_quantity * ABS(sls_price)
                 ELSE sls_sales
            END,
            sls_quantity,
            CASE WHEN sls_price = 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0) ELSE ABS(sls_price) END
        FROM bronze.crm_sales_details;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into crm_sales_details in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading crm_sales_details: %', SQLERRM;
    END;

    -- erp_cust_az12----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.erp_cust_az12...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_cust_az12;

        INSERT INTO silver.erp_cust_az12
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4) ELSE cid END,
            CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into erp_cust_az12 in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    -- erp_loc_a101----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.erp_loc_a101...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_loc_a101;

        INSERT INTO silver.erp_loc_a101
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'n/a'
                ELSE cntry
            END
        FROM bronze.erp_loc_a101;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into erp_loc_a101 in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    -- erp_px_cat_g1v2----------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE 'Loading silver.erp_px_cat_g1v2...';
        start_time := clock_timestamp();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;

        INSERT INTO silver.erp_px_cat_g1v2
        SELECT * FROM bronze.erp_px_cat_g1v2;

        GET DIAGNOSTICS rows_loaded = ROW_COUNT;
        end_time := clock_timestamp();
        RAISE NOTICE 'Loaded % rows into erp_px_cat_g1v2 in % seconds.', rows_loaded, EXTRACT(EPOCH FROM end_time - start_time);
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;
    -- Final total time
    total_end := clock_timestamp();
    RAISE NOTICE 'Total load time: % seconds.', EXTRACT(EPOCH FROM total_end - total_start);
END;
$BODY$;
ALTER PROCEDURE silver.load_silver()
    OWNER TO postgres;
--------------------------------------------------------------------------------
/*
OUTPUT:
NOTICE:  Starting silver layer load...
NOTICE:  Loading silver.crm_cust_info...
NOTICE:  Loaded 18485 rows into crm_cust_info in 0.134139 seconds.
NOTICE:  Loading silver.crm_prd_info...
NOTICE:  Loaded 397 rows into crm_prd_info in 0.009146 seconds.
NOTICE:  Loading silver.crm_sales_details...
NOTICE:  Loaded 60398 rows into crm_sales_details in 0.932120 seconds.
NOTICE:  Loading silver.erp_cust_az12...
NOTICE:  Loaded 18484 rows into erp_cust_az12 in 0.138993 seconds.
NOTICE:  Loading silver.erp_loc_a101...
NOTICE:  Loaded 18484 rows into erp_loc_a101 in 0.198159 seconds.
NOTICE:  Loading silver.erp_px_cat_g1v2...
NOTICE:  Loaded 37 rows into erp_px_cat_g1v2 in 0.002208 seconds.
NOTICE:  Total load time: 1.420807 seconds.
CALL

Query returned successfully in 1 secs 565 msec.
*/


