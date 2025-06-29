/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/


Use DataWarehouse;

-- Drop the table if it exists
IF OBJECT_ID('bronze.crm_cust_info', 'U') is NOT NULL -- 'U' stands for user defined table
	DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info(

	cst_id 			INT,
	cst_key			NVARCHAR(50),
	cst_firstname 		NVARCHAR(50),
	cst_lastname 		NVARCHAR(50),
	cst_marital_status 	NVARCHAR(50),
	cst_gndr 		NVARCHAR(50),
	cst_create_date 	DATE

);

IF OBJECT_ID('bronze.crm_prd_info', 'U') is NOT NULL 
	DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info(

	prd_id		INT,
	prd_key		NVARCHAR(50),
	prd_nm 		NVARCHAR(50),
	prd_cost	INT,
	prd_line 	NVARCHAR(50),	
	prd_start_dt 	DATE,
	prd_end_dt 	DATE

);

IF OBJECT_ID('bronze.crm_sales_details', 'U') is NOT NULL 
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details(

	sls_ord_num	NVARCHAR(50),
	sls_prd_key	NVARCHAR(50),
	sls_cust_id	INT,
	sls_order_dt 	INT,
	sls_ship_dt 	INT,
	sls_due_dt	INT,
	sls_sales 	INT,
	sls_quantity 	INT,
	sls_price 	INT

);


IF OBJECT_ID('bronze.erp_cust_az12', 'U') is NOT NULL 
	DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12(

	cid 	NVARCHAR(50),
	bdate 	DATE,
	gen 	NVARCHAR(50)

);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') is NOT NULL 
	DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101(

	cid 	NVARCHAR(50),
	cntry 	NVARCHAR(50)

);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') is NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(

	id 		NVARCHAR(50),
	cat 		NVARCHAR(50),
	subcat 		NVARCHAR(50),
	maintenance	NVARCHAR(50)

);

------------------------------------------------------------------------
/*Accidently created the tables with name in upper case, then to make it lowercase used sp_rename 
**sp_rename** is a system stored procedure in SQL Server used to rename database objects, such as:

- Tables

- Columns

- Indexes

- Constraints

- Stored procedures, etc.

EXEC sp_rename 'bronze.erp_PX_CAT_G1V2','bronze.erp_px_cat_g1v2';

EXEC sp_rename 'bronze.erp_LOC_A101','bronze.erp_loc_a101';

EXEC sp_rename 'bronze.erp_CUST_AZ12','bronze.erp_cust_az12';

*/
