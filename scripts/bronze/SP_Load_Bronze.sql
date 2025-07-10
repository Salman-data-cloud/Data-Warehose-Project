/* This is the stored procedure of the load of Bronze layer. We can execute it like:
EXEC bronze.load_bronze
*/

USE DataWarehouse;
Go
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '=======================';
		PRINT 'LOADING THE BROZE LAYER';
		PRINT '=======================';

		PRINT '=======================';
		PRINT 'LOADING CRM SECTION';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		PRINT '>> INSERING DATA INTO bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		PRINT '>> INSERING DATA INTO bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		PRINT '>> INSERING DATA INTO bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

		PRINT '=======================';
		PRINT 'LOADING ERP SECTION';
		PRINT '=======================';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.erm_CUST_AZ12';
		truncate table bronze.erm_CUST_AZ12;
		PRINT'>> INSERTING DATA INTO bronze.erm_CUST_AZ12';
		BULK INSERT bronze.erm_CUST_AZ12
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.erm_LOC_A101';
		truncate table bronze.erm_LOC_A101;
		PRINT'>> INSERTING DATA INTO bronze.erm_LOC_A101';
		BULK INSERT bronze.erm_LOC_A101
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';
		
		SET @start_time = GETDATE();
		PRINT'>> TRUNCATING TABLE: bronze.erm_PX_CAT_G1V2';
		truncate table bronze.erm_PX_CAT_G1V2;
		PRINT'>> INSERTING DATA INTO bronze.erm_PX_CAT_G1V2';
		BULK INSERT bronze.erm_PX_CAT_G1V2
		FROM 'C:\Users\salma\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';
		PRINT '####################################################################'
		SET @batch_end_time = GETDATE();
		PRINT'>> LOADING FULL BRONZE LAYER COMPLETED!'
		PRINT'>> TIME NEEDED TO LOAD THE FULL BRONZE LAYER: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+ 'seconds.'
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	END CATCH
	
END
