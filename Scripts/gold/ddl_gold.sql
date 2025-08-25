/*
================================================================================
DDL Script: Create Gold Views
================================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;

GO

CREATE VIEW gold.dim_customers AS
	SELECT 

		ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key, -- surrogate key
		ci.cst_id AS customer_id,
		ci.cst_key AS customer_number,
		ci.cst_firstname AS first_name,
		ci.cst_lastname AS last_name,
		el.cntry AS country,
		ci.cst_marital_status AS marital_status,
		CASE WHEN ci.cst_gndr = 'UNKNOWN' THEN ISNULL(ec.gen,'UNKNOWN')  -- CRM is the master table
			ELSE ci.cst_gndr
		END AS gender,
		ec.bdate AS birth_date,
		ci.cst_create_date AS created_date

	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ec
	ON ci.cst_key = ec.cid
	LEFT JOIN silver.erp_loc_a101 el
	ON ci.cst_key = el.cid

GO

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;

GO

CREATE VIEW gold.dim_products AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cp.prd_start_dt,cp.prd_key) AS product_key, -- surrogate key using prd_key as tie breaker.
		cp.cat_id AS category_id,
		ep.cat AS category_name,
		ep.subcat AS subcategory,
		cp.prd_id AS product_id,
		cp.prd_key AS product_number,
		cp.prd_nm AS product_name,
		cp.prd_line AS product_line,
		cp.prd_cost AS cost,
		ep.maintenance,
		cp.prd_start_dt AS start_date

	FROM silver.crm_prd_info cp
	LEFT JOIN silver.erp_px_cat_g1v2 ep
	ON cp.cat_id = ep.id
	where cp.prd_end_dt IS NULL -- filtering current data alone

GO

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;

GO

CREATE VIEW gold.fact_sales AS
	SELECT 

		sd.sls_ord_num AS order_number,
		gp.product_key,
		dc.customer_key,
		sd.sls_order_dt AS order_date,
		sd.sls_ship_dt AS ship_date,
		sd.sls_due_dt AS due_date,
		sd.sls_sales AS sales_amount,
		sd.sls_quantity AS quantity,
		sd.sls_price AS price

	FROM silver.crm_sales_details sd
	LEFT JOIN gold.dim_customers dc
	ON sd.sls_cust_id = dc.customer_id
	LEFT JOIN gold.dim_products gp
	ON sd.sls_prd_key = gp.product_number
