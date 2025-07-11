/*

==========================================================================================================

Stored Procedure:Load Silver Layer (Bronze -> Silver)

==========================================================================================================

Script Objective:
      This stored procedure loads data into silver schema from bronze schema.
      It performs the following:
                  Truncates the silver tables before loading.
                  Then INSERT standardized data into silver tables
      To execute the Stored Procedure:
                                      EXEC bronze.load_bronze;
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_end_time DATETIME, @batch_start_time DATETIME;
		BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================================='
		PRINT 'Loading the Silver Layer';
		PRINT '=========================================================='

		PRINT '----------------------------------------------------------'
		PRINT 'Loading the CRM Tables';
		PRINT '-----------------------------------------------------------'
SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_cust_info';
TRUNCATE TABLE silver.crm_cust_info;
PRINT '>> INSERTING DATA INTO: silver.crm_cust_info';

INSERT INTO silver.crm_cust_info (cst_id,
cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
SELECT 
cst_id,
cst_key,

TRIM(cst_firstname) AS cst_firstname, -- Remove unwanted spaces
TRIM(cst_lastname) AS cst_lastname,

CASE WHEN UPPER(TRIM(cst_marital_status)) IS NULL THEN 'n/a'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
END cst_marital_status,
CASE WHEN UPPER(TRIM(cst_gndr)) IS NULL THEN 'n/a' 
	 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
END cst_gndr,
cst_create_date
FROM (SELECT *,ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last --CREATE A Unique number for each row in the table seperate the table through customer id and order it descendingly with create date
FROM bronze.crm_cust_info 
WHERE cst_id IS NOT NULL
)t where flag_last =1 --Select the most recent info about the customer to remover duplicates
SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_prd_info';
TRUNCATE TABLE silver.crm_prd_info;
PRINT '>> INSERTING DATA INTO: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info(
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
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(Partition BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_date
FROM bronze.crm_prd_info
SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> INSERTING DATA INTO: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details(sls_ord_num,
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
CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS date)
END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS date)
END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
	 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS date)
END AS sls_due_dt,

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity *ABS(sls_price) -- To avoid negative values
	 ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity,0) -- To avoid division with Zero
	 ELSE sls_price
END AS sls_price

FROM bronze.crm_sales_details
SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';
		PRINT '----------------------------------------------------------'
		PRINT 'Loading the ERP Tables';
		PRINT '-----------------------------------------------------------'

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> INSERTING DATA INTO: silver.erp_cust_az12';

INSERT INTO silver.erp_cust_az12(cid, bdate,gen)
SELECT 
CASE WHEN cid LIKE 'NAS%' -- Removing the NAS from the cid column to match it with customer info table key
	 THEN SUBSTRING(cid, 4, LEN(cid))
	 ELSE cid
END AS cid,
CASE WHEN bdate> GETDATE() THEN NULL -- Set future birthdates to NULL
	 ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('M', 'Male') THEN 'Male' -- Handling gender values
	 WHEN UPPER(TRIM(gen)) IN ('F', 'Female') THEN 'Female'
	 ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_loc_a101';
TRUNCATE TABLE silver.erp_loc_a101;
PRINT '>> INSERTING DATA INTO: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101(cid,cntry)

SELECT REPLACE(cid,'-','') AS cid,
CASE WHEN UPPER(TRIM(cntry)) IN ('US','USA') THEN 'United States'
	 WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
	 WHEN UPPER(TRIM(cntry)) = '' OR UPPER(TRIM(cntry)) IS NULL THEN 'n/a'
	 ELSE cntry
END AS cntry
FROM bronze.erp_loc_a101

SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';

SET @start_time = GETDATE();
PRINT '>> TRUNCATING TABLE: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> INSERTING DATA INTO: silver.erp_px_cat_g1v2';

INSERT INTO silver.erp_px_cat_g1v2 (
id, 
cat,
subcat,
maintenance)
SELECT 
id, 
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
SET @end_time = GETDATE();
PRINT 'Load Duration: '+ CAST ( DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + ' second(s)';
		PRINT '---------------------------------------';
SET @batch_end_time = GETDATE();
		PRINT 'Silver Loading Complete : '
		PRINT '- Total Duration: '+ CAST ( DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + ' second(s)';
		PRINT '==========================================================';
		END TRY

BEGIN CATCH
		PRINT '==========================================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST( ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error MEssage' + CAST( ERROR_STATE() AS NVARCHAR);
		PRINT '==========================================================='
	END CATCH
END
