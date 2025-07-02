/*
===============================================================================
Stored Procedure: Load Bronze Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN
  	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
  	SET @batch_start_time = GETDATE();
  	BEGIN TRY
  
  		SET @batch_start_time = GETDATE();
  		PRINT '=========================================================================';
  		PRINT '							Loading Silver Layer			 ';
  		PRINT '=========================================================================';
  
              PRINT '------------------------------------------';
              PRINT '			Loading CRM Tables				 ';
              PRINT '------------------------------------------';
  
     SET @start_time = GETDATE();
          		PRINT ' >> Truncating Table: silver.crm_cust_info ';
          		TRUNCATE TABLE silver.crm_cust_info;
          		PRINT ' >> Inserting Data Into: silver.crm_cust_info ';
          		INSERT INTO silver.crm_cust_info(
          
          			cst_id,
          			cst_key,
          			cst_firstname,
          			cst_lastname,
          			cst_marital_status,
          			cst_gndr,
          			cst_create_date
          
          		)
          
          		SELECT 
          			cst_id,
          			cst_key, 
          
          			--- Removes unwanted space to maintain consitency and uniformity across all records
          
          			TRIM(cst_firstname) AS cst_firstname, 
          			TRIM(cst_lastname) AS cst_lastname,
          
          			--- Data Normalization or Standardization
          			--- Maps code values to maningful, user friendly description
          
          			CASE 
          				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
          				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
          				 ELSE 'UNKNOWN' -- Handling Missing values
          
          			END AS cst_marital_status,
          			CASE
          				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
          				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
          				 ELSE 'UNKNOWN'
          
          			END AS cst_gndr,
          			cst_create_date
          
          		FROM 
          		(
          		----- Removing duplicates and ensure to have one record per entity by retaining the lastest record
          			SELECT 
          			*,
          			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last_date
          			FROM bronze.crm_cust_info
          			where cst_id IS NOT NULL
          		)t
          		where flag_last_date = 1 ;
          
     SET @end_time = GETDATE();
     PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
     PRINT '------------------------'
      
      		-------------crma-prd
      
     SET @start_time = GETDATE();
          		PRINT ' >> Truncating Table: silver.crm_prd_info ';
          		TRUNCATE TABLE silver.crm_prd_info;
          		PRINT ' >> Inserting Data Into: silver.crm_prd_info ';
          		INSERT INTO silver.crm_prd_info (
          
            			prd_id,
            			cat_id,
            			prd_key,
            			prd_nm,
            			prd_cost,
            			prd_line,
            			prd_start_dt,
            			prd_end_dt
          
          		)
          		SELECT 
          
            			prd_id,
            			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- extracting and manipulating cat_id from prd_key to join with "erp_px_cat_g1v2" table
            			SUBSTRING(prd_key,7,len(prd_key)) AS prd_key,
            			prd_nm,
            			COAlESCE(prd_cost,0) AS prd_cost, -- converting null to 0 provided business allowing
            			CASE UPPER(prd_line)
            
            				WHEN 'M' THEN 'Mountain'
            				WHEN 'S' THEN 'Sport'
            				WHEN 'R' THEN 'Road'
            				WHEN 'T' THEN 'Touring'
            				ELSE 'UNKNOWN'
            
            			END AS prd_line,
            			prd_start_dt,
            			DATEADD(DAY,-1,LEAD(prd_start_dt) OVER(PARTITION BY prd_key order by prd_start_dt)) AS prd_end_dt
            
            		FROM bronze.crm_prd_info;
      
      SET @end_time = GETDATE()
      PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
      PRINT '------------------------'
      
      		-----------------crm sales
      	
      SET @start_time =GETDATE();
      		PRINT ' >> Truncating Table: silver.crm_sales_details ';
      		TRUNCATE TABLE silver.crm_sales_details;
      		PRINT ' >> Inserting Data Into: silver.crm_sales_details ';
      		INSERT INTO silver.crm_sales_details (
      
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
      
      		SELECT 
      
          			 sls_ord_num,
          			 sls_prd_key,
          			 sls_cust_id,
          
          			 CASE
          	 
          				WHEN sls_order_dt <=0 OR LEN(sls_order_dt) !=8  -- Handling invalid data
          				THEN NULL
          				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)  -- converting string to date
          
          			 END AS sls_order_dt, 
          			 CASE
          
          				WHEN sls_ship_dt <=0 OR LEN(sls_ship_dt) !=8
          				THEN NULL
          				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) 
          
          			 END AS sls_ship_dt, 
          			 CASE
          	  
          				WHEN sls_due_dt <=0 OR LEN(sls_due_dt) !=8
          				THEN NULL
          				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) 
          
          			 END AS sls_due_dt,
          			 CASE
          	 
          				WHEN sls_sales != ABS(sls_quantity*sls_price) OR sls_sales IS NULL OR sls_sales < 0 -- Checking business rule,handling Missing data,Invalid data
          				THEN ABS(sls_quantity * sls_price)
          				ELSE sls_sales
          
          			 END AS sls_sales,
          
          			 /*we dont check for sls_price != ABS(sls_sales/NULLIF(sls_quantity,0)) or sls_quantity != ABS(sls_sales/NULLIF(sls_price,0)) because price & quantity is indepedent data 
          			 means directly comes from source but sales is calculated data
          			 so always tend to correct dependent data 
          			 */
          
          			 CASE
          	 
          				WHEN sls_quantity IS NULL OR sls_quantity <= 0  -- Handling Missing and Invalid data
          				THEN ABS(sls_sales/NULLIF(sls_price,0))
          				ELSE sls_quantity
          
          			 END AS sls_quantity,
          			 CASE
          	 
          				WHEN sls_price IS NULL OR sls_price <= 0  
          				THEN ABS(sls_sales/NULLIF(sls_quantity,0))
          				ELSE sls_price
          
          			 END AS sls_price
          
        
        		  FROM bronze.crm_sales_details;
      
      SET @end_time = GETDATE();
      PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
      PRINT '------------------------'

          
          		PRINT '------------------------------------------';
          		PRINT '			Loading ERP Tables				 ';
          		PRINT '------------------------------------------';
  
  		SET @start_time = GETDATE();
      		PRINT ' >> Truncating Table: silver.erp_cust_az12 ';
      		TRUNCATE TABLE silver.erp_cust_az12;
      		PRINT ' >> Inserting Data Into: silver.erp_cust_az12 ';
      		INSERT INTO silver.erp_cust_az12 (
      
      			cid,
      			bdate,
      			gen
      
      		)
      
      		SELECT
      
        			CASE 
        		
        				WHEN cid LIKE 'NAS%' THEN SUBSTRING(TRIM(cid),4,LEN(TRIM(cid))) -- Handling inconsistent data
        				ELSE TRIM(cid)
        
        			END AS cid,
        			CASE
        	
        				WHEN bdate > GETDATE() THEN NULL --  Handling Out of bound dates
        				ELSE bdate
        
        			END AS bdate,
        			CASE 
        
        				WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female' -- data standardizantion and uniformity
        				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        				ELSE 'UNKNOWN'
        
        			END AS gen
        
        		FROM bronze.erp_cust_az12;
    
  		SET @end_time = GETDATE();
  		PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '------------------------'
  
  
  
  		-----erp_loc
  
  		SET @start_time = GETDATE();
      		PRINT ' >> Truncating Table: silver.erp_loc_a101 ';
      		TRUNCATE TABLE silver.erp_loc_a101;
      		PRINT ' >> Inserting Data Into: silver.erp_loc_a101 ';
      		INSERT INTO silver.erp_loc_a101 (
      
        			cid,
        			cntry
      		)
      
      		SELECT 
      
        			REPLACE(cid,'-','') AS cid, -- Removing '-' from the value to match with cust table in crm
        			CASE
        				 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        				 WHEN TRIM(cntry) = 'US' OR TRIM(cntry) = 'USA' THEN 'United States'
        				 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'UNKNOWN'
        				 ELSE cntry
        
        			END AS cntry -- Handle missing data, Data standardization and uniformity
      
      		FROM bronze.erp_loc_a101;
    
    SET @end_time = GETDATE();
    PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
    PRINT '------------------------'

  
  
  		SET @start_time = GETDATE();
  		PRINT ' >> Truncating Table: silver.erp_px_cat_g1v2 ';
  		TRUNCATE TABLE silver.erp_px_cat_g1v2;
  		PRINT ' >> Inserting Data Into: silver.erp_px_cat_g1v2 ';
    		INSERT INTO silver.erp_px_cat_g1v2 (
    			id,
    			cat,
    			subcat,
    			maintenance
    		)
    
    		SELECT
    
    			id,
    			cat,
    			subcat,
    			maintenance
    
    		FROM bronze.erp_px_cat_g1v2;
  
  		SET @end_time = GETDATE();
  		PRINT ' >> Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' seconds';
  		PRINT '------------------------'
  		SET @batch_end_time = GETDATE();
  
  		PRINT '=========================================='
  		PRINT 'Loading Silver Layer Completed'
  		PRINT '	- Total Duration: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' seconds'
  		PRINT '=========================================='
  
  	END TRY
  
  	BEGIN CATCH
  	
  			PRINT '=============================================='
  			PRINT 'Error Occured During Loading Silver Layer	 '
  			PRINT 'Error Message ' + CAST(ERROR_Number() AS NVARCHAR) --- these Error_Number & Error_State only catch loading error means data type mismatch
  			PRINT 'Error Message ' + CAST(ERROR_State() AS NVARCHAR) -- difference in column etc .. but not the file missing or data missing errors
  			PRINT '=============================================='
  
  	END CATCH
END
