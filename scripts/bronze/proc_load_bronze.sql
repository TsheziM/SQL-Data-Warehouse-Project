/*

==========================================================================================================

Stored Procedure:Load Bronze Layer (source -> Bronze)

==========================================================================================================

Script Objective:
      This stored procedure loads data into bronze schema from external CSV files.
      It performs the following:
                  Truncates the bronze tables before loading.
                  Uses the BULK INSERT command to load data from CSV files to bronze tables
      To execute the Stored Procedure:
                                      EXEC bronze.load_bronze;
*/


--CREATE Stored procedures

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================='
		PRINT 'Loading the Bronze Layer';
		PRINT '=========================================================='

		PRINT '----------------------------------------------------------'
		PRINT 'Loading the CRM Tables';
		PRINT '-----------------------------------------------------------'
		
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into: bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

		PRINT '----------------------------------------------------------'
		PRINT 'Loading the ERP Tables';
		PRINT '-----------------------------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>> Inserting data into: bronze.erp_CUST_AZ12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101

		PRINT '>> Inserting data into: bronze.erp_LOC_A101'
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2

		PRINT '>> Inserting data into: bronze.erp_PX_CAT_G1V2'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\USER\Documents\Projects\Data Engineering Project\SQL Datawarehouse Project\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2, -- Starts recording data from 2nd raw
			FIELDTERMINATOR = ',', -- Acts as a delimiter specifyer
			TABLOCK --Locks the entire table while loading it
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';
		SET @batch_end_time = GETDATE();
		PRINT 'Bronze Loading Complete : '
		PRINT '- Total Duration: '+ CAST ( DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' second(s)';
		PRINT '==========================================================';


	END TRY
	BEGIN CATCH
		PRINT '==========================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error MEssage' + CAST( ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================='
	END CATCH
END

EXEC bronze.load_bronze
