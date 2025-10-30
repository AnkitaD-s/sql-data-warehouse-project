/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    First on is to creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 
    Second script sets up three schemas 
    within the database: 'bronze', 'silver' and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

/*In PostgreSQL, there isnt any direct equivalent to the **USE** database_name command found in MySQL.
Create two separate SQL files:
1) setup_drop_create.sql 
(You must run this script from another database, like postgres, because you can't drop/create a database you're currently connected to.)
2) setup_schemas.sql (please run from the created database datawarehouse)
*/

--setup_drop_create.sql 
-- Step 1: Terminate connections to the target database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- Step 2: Drop the database (run this from a different database)
DROP DATABASE IF EXISTS datawarehouse;

-- Step 3: Create the new database
CREATE DATABASE datawarehouse;

--setup_schemas.sql

-- Create Schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
