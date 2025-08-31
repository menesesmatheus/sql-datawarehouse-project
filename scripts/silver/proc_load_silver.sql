/*
==========================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
==========================================================

==========================================================
Script Purpose:
	This Stored Procedure loads data into the silver schema from bronze schema.

Parameters:
	None.

Usage Exemple:
	EXEC silver.load_silver;
==========================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	BEGIN TRY
		DECLARE @start_tm DATETIME,@end_tm DATETIME, @begin_pcdr DATETIME, @end_pcdr DATETIME;
		SET @begin_pcdr = GETDATE();
		PRINT '========================================================';
		PRINT 'LOADING DATA FROM BRONZE TO SILVER';
		PRINT '========================================================';

		SET @start_tm = GETDATE();	
		PRINT '>> TRUNCATING silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> INSERTING DATA ON silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (cst_id, cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
			SELECT
				cst_id,
				cst_key,
				TRIM(cst_firstname) AS cst_firstname,
				TRIM(cst_lastname) AS cst_lastname,
		
					CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						 Else 'n/a'
					END AS cst_marital_status, --Normalize values to readable formats for marital status
		
					CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						 Else 'n/a'
					END  as cst_gndr, --Normalize values to readable formats for gender values

				cst_create_date
			FROM(
				SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_Date DESC) flag_id
				FROM bronze.crm_cust_info
				WHERE cst_id IS NOT NULL
			)t WHERE flag_id = 1 -- Select the most recent record per customer
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @start_tm = GETDATE();
		PRINT '>> TRUNCATING silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> INSERTING DATA ON silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
			SELECT
				prd_id,
				REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id, --Extract product category
				SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key, --Extract product key
				prd_nm,
				ISNULL(prd_cost,0) AS prd_cost,

				CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Montain'
					 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
					 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
					 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
					 ELSE 'n/a'
				END AS prd_line, ----Normalize values to readable formats for product line

				CAST(prd_start_dt AS DATE) AS prd_start_dt,
				CAST(
					LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE
					) AS prd_end_dt
			FROM bronze.crm_prd_info
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @start_tm = GETDATE();
		PRINT '>> TRUNCATING silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> INSERTING DATA ON silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details (sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)
			SELECT 
				sls_ord_num,
				sls_prd_key,
				sls_cust_id,
	
				CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt, --Verify dates and cast then as DATE

				CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt, --Verify dates and cast then as DATE

				CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt, --Verify dates and cast then as DATE

				CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
					 THEN sls_quantity * ABS(sls_price)
					 ELSE sls_sales
				END sls_sales, -- Correct and Redo the sales based on business rules

				sls_quantity,

				CASE WHEN sls_price IS NULL OR sls_price <= 0 
					 THEN sls_sales / NULLIF(sls_quantity, 0)
					 ELSE sls_price
				END sls_price -- Correct and Redo the price based on business rules

			FROM bronze.crm_sales_details
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @start_tm = GETDATE();
		PRINT '>> TRUNCATING silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> INSERTING DATA ON silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
			SELECT
				CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
					 ELSE cid
				END cid, -- Remove the NAS prefix

				CASE WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
				END bdate, -- Set the future bdays to NULL

				CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
					 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
					 Else 'n/a'
				END gen -- Normalize gender values and handle unknow cases

			FROM bronze.erp_cust_az12
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @start_tm = GETDATE();
		PRINT '>> TRUNCATING silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> INSERTING DATA ON silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(cid,cntry)
			SELECT 
				REPLACE(cid, '-','') cid, -- Clean the - from the id

				CASE WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
					 ELSE TRIM(cntry)
				END cntry -- TRIM and handle multiple options for the same country

			FROM bronze.erp_loc_a101
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @start_tm = GETDATE();
		PRINT '>> TRUNCATING silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> INSERTING DATA ON silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
			SELECT
				id,
				cat,
				subcat,
				maintenance
			FROM bronze.erp_px_cat_g1v2
		SET @end_tm = GETDATE();
		PRINT '>> DURATION: ' + CAST(DATEDIFF(second,@start_tm,@end_tm) AS VARCHAR) + ' seconds';
		PRINT '---------------------------------------------------------';

		SET @end_pcdr = GETDATE();
		PRINT '========================================================';
		PRINT 'SILVER LAYER LOAD COMPLETE';
		PRINT 'BATCH DURATION: ' + CAST(DATEDIFF(second,@begin_pcdr,@end_pcdr) AS VARCHAR) + ' seconds';
		PRINT '========================================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR DURING	SILVER LAYER LOAD';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR(50));
		PRINT '==============================================';
	END CATCH
END
