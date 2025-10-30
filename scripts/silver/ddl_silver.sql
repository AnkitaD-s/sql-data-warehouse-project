-- Table: silver.crm_cust_info
-- DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE IF NOT EXISTS silver.crm_cust_info
(
    cst_id int,
    cst_key varchar(50),
    cst_firstname varchar(50),
    cst_lastname varchar(50),
    cst_marital_status varchar(50) ,
    cst_gndr varchar(50) ,
    cst_create_date date,
    dwh_create_date timestamp  DEFAULT CURRENT_DATE
);
-- Table: silver.crm_prd_info
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE IF NOT EXISTS silver.crm_prd_info
(
    prd_id int,
	cat_id varchar(50) ,
    prd_key varchar(50) ,
    prd_nm varchar(50) ,
    prd_cost int,
    prd_line varchar(50) ,
    prd_start_dt date,
    prd_end_dt date,
    dwh_create_date timestamp  DEFAULT CURRENT_DATE
);
-- Table: silver.crm_sales_details
-- DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE IF NOT EXISTS silver.crm_sales_details
(
    sls_ord_num varchar(50) ,
    sls_prd_key varchar(50) ,
    sls_cust_id int,
    sls_order_dt int,
    sls_ship_dt int,
    sls_due_dt int,
    sls_sales int,
    sls_quantity int,
    sls_price int,
    dwh_create_date timestamp DEFAULT CURRENT_DATE
);
-- Table: silver.erp_cust_az12
-- DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE IF NOT EXISTS silver.erp_cust_az12
(
    cid varchar(50) ,
    bdate date,
    gen varchar(50) ,
    dwh_create_date timestamp  DEFAULT CURRENT_DATE
);
-- Table: silver.erp_loc_a101
-- DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE IF NOT EXISTS silver.erp_loc_a101
(
    cid varchar(50) ,
    cntry varchar(50) ,
    dwh_create_date timestamp  DEFAULT CURRENT_DATE
);
-- Table: silver.erp_px_cat_g1v2
-- DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS silver.erp_px_cat_g1v2
(
    id varchar(50) ,
    cat varchar(50) ,
    subcat varchar(50) ,
    maintenance varchar(50) ,
    dwh_create_date timestamp DEFAULT CURRENT_DATE
);
