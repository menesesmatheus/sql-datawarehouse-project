/*
==========================================================
Stored Procedure: Load Bronze Layer (Source --> Bronze)
==========================================================

==========================================================

Script Purpose:
	This Stored Procedure loads data into the bronze schema from external CSV files.

Parameters:
	None.

Usage Exemple:
	EXEC bronze.load_bronze;
==========================================================
*/




/*
Procedure to truncate and bulk Insert of the data on the tables
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		DECLARE @start_tm DATETIME, @end_tm DATETIME, @batch_start_tm DATETIME, @batch_end_tm DATETIME;
		SET @batch_start_tm = GETDATE();
		PRINT '======================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '======================================================';

		--CRM
		PRINT '------------------------------------------------------';
		PRINT 'LOADING CRM';
		PRINT '------------------------------------------------------';
				
		--cust_info
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';


		--prd_info
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';

		
		--sales_details
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';

		--ERP
		PRINT '------------------------------------------------------';
		PRINT 'LOADING ERP';
		PRINT '------------------------------------------------------';

		
		--cust_az12
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';

		
		--LOC_A101
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';

		--px_cat_g1v2
		SET @start_tm = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> TRUNCATING TABLE';
		PRINT '>> INSERTING DATA';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Matheus\Documents\sql\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv' 
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_tm = GETDATE();
		PRINT '>> LOAD DURANTION: ' + CAST (DATEDIFF(second, @start_tm, @end_tm) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------------';

		SET @batch_end_tm = GETDATE();
		PRINT '==========================================';
		PRINT 'BRONZE LAYER LOAD COMPLETE';
		PRINT 'BATCH DURATION: ' + CAST(DATEDIFF(second,@batch_start_tm,@batch_end_tm) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR DURING BRONZE LAYER LOAD';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' + CAST (ERROR_NUMBER() AS NVARCHAR(50));
		PRINT '==============================================';
	END CATCH
END
