/*
===================================================================================================
Stored Procedure: Load Silver Layer (Source -> Silver)
===================================================================================================
Script Purpose:
    This stored procedure loads data into the 'silver' schema from external CSV files.
    It performs the following actions:
     - Truncates the silver tables before loading data.
     - Uses the 'BULK INSERT' command to load data from csv files to silver tables.

Parameters:
    None.
    This stored parameter does not accept my parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
======================================================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
BEGIN TRY
SET @batch_start_time = GETDATE();
PRINT '=========================================';
PRINT 'Loading Bronze Layer';
PRINT '=========================================';

PRINT '-----------------------------------------';
PRINT 'Loading CRM Tables';
PRINT '-----------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.crm_cust_info';
TRUNCATE TABLE  silver.crm_cust_info; 

PRINT '>>Inserting Data Into:silver.crm_cust_info';
BULK INSERT silver.crm_cust_info
FROM'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
WITH(
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

---FOR SECOND TABLE---
SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.crm_prd_info';
TRUNCATE TABLE  silver.crm_prd_info; 

PRINT '>>Inserting Data Into:silver.crm_prd_info';
BULK INSERT silver.crm_prd_info
FROM'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

---for third table
SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.crm_sales_details';
TRUNCATE TABLE  silver.crm_sales_details; 

PRINT '>>Inserting Data Into:silver.crm_sales_details';
BULK INSERT silver.crm_sales_details
FROM'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

---for fourth table


PRINT '-----------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '-----------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.erp_loc_a101';
TRUNCATE TABLE  silver.erp_loc_a101;

PRINT '>>Inserting Data Into:silver.erp_loc_a101';
BULK INSERT silver.erp_loc_a101
FROM'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\loc_a101.CSV'
WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

---for fifth table
SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;

PRINT '>>Inserting Data Into:silver.erp_cust_az12';
BULK INSERT silver.erp_cust_az12
FROM'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\cust_az12.CSV'
WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

---for sixth table
SET @start_time = GETDATE();
PRINT '>> Truncating Table:silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;

PRINT '>>Inserting Data Into:silver.erp_px_cat_g1v2';
BULK INSERT silver.erp_px_cat_g1v2
FROM 'C:\Users\Jhansi\Downloads\f78e076e5b83435d84c6b6af75d8a679\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.CSV'
WITH (
     FIRSTROW = 2,
     FIELDTERMINATOR = ',',
     TABLOCK
);
SET @start_time = GETDATE();
PRINT '>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds';
PRINT '---------------------------'

SET @batch_end_time = GETDATE();
PRINT '======================================'
PRINT 'Loading Bronze Layer is Completed';
PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
PRINT '======================================'
END TRY
BEGIN CATCH
PRINT '=================================================================';
PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR); 
END CATCH
END
