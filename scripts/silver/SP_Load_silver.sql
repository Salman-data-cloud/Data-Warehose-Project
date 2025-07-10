/*This is the store procedure of the silver layer. Here it first truncates all the tables and then insert all the values that are transformed and cleaned
from the bronze layer. 
Example usage: EXEC silver.load_silver*/
USE DataWarehouse
Go
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	SET @batch_start_time = GETDATE();
    BEGIN TRY
		    PRINT '=======================';
		    PRINT 'LOADING THE SILVER LAYER';
		    PRINT '=======================';

		    PRINT '=======================';
		    PRINT 'LOADING CRM SECTION';
		    PRINT '=======================';

		    SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT('... LOADING DATA IN SILVER.CRM_CUST_INFO...')
        INSERT INTO silver.crm_cust_info(
	        cst_id,
	        cst_key,
	        cst_firstname,
	        cst_lastname,
	        cst_marital_status,
	        cst_gndr,
	        cst_create_date)

        select 
        cst_id, 
        cst_key, 
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname, 
        case when upper(trim(cst_marital_status)) = 'S' then 'Single'
	        when upper(trim(cst_marital_status)) = 'M' then 'Married'
	        else 'N/A'
        end cst_marital_status,
        case when upper(trim(cst_gndr)) = 'F' then 'Female'
	        when upper(trim(cst_gndr)) = 'M' then 'Male'
	        else 'N/A'
        end cst_gndr,
        cst_create_date 
        from (select *, row_number() over (partition by cst_id order by cst_create_date desc) as flag_last from bronze.crm_cust_info where cst_id is not null) temp where flag_last =1;
        SET @end_time = GETDATE();
		    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT('... LOADING DATA IN SILVER.CRM_PRD_INFO...')
        INSERT INTO silver.crm_prd_info(
            prd_id,
            category_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5), '-','_') as category_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key,
        prd_nm,
        ISNULL(prd_cost,0) AS prd_cost, -- IF NULL, THEN MAKE IT ZERO (0)
        CASE UPPER(TRIM(prd_line))
             WHEN 'M' then 'Metro'
             WHEN 'R' then 'Rural'
             WHEN 'S' then 'Suburb'
             WHEN 'T' then 'Town'
             ELSE 'N/A'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_date,
        CAST(DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
        FROM bronze.crm_prd_info
        SET @end_time = GETDATE();
	    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT('... LOADING DATA IN SILVER.CRM_SALES_DETAILS...')
        INSERT INTO silver.crm_sales_details(
        sls_ord_num,
        sls_prd_key,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)

        SELECT 
        sls_ord_num,
        sls_prd_key,
        CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt)!=8 THEN NULL
             ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
        END sls_order_dt,
        CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt)!=8 THEN NULL
             ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
        END sls_ship_dt,
        CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt)!=8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
        END sls_due_dt,
        CASE WHEN sls_sales<=0 or sls_sales is null or sls_sales!= sls_quantity*ABS(sls_price) THEN sls_quantity*ABS(sls_price)
        ELSE sls_sales
        END sls_sales,
        CASE WHEN sls_quantity<0 THEN ABS(sls_quantity)
             WHEN sls_quantity is null THEN 0
             ELSE sls_quantity
        END sls_quantity,
        CASE WHEN sls_price<=0 or sls_price is null THEN sls_sales/NULLIF(sls_quantity,0)
     
             ELSE sls_price
        END sls_price     
        FROM bronze.crm_sales_details
        SET @end_time = GETDATE();
	    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

        PRINT '=======================';
		PRINT 'LOADING CRM SECTION';
		PRINT '=======================';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erm_CUST_AZ12;
        PRINT('... LOADING DATA IN SILVER.ERM_CUST_AZ12...')
        INSERT INTO silver.erm_CUST_AZ12(
        CID,
        BDATE,
        GEN)

        SELECT 
        CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	         ELSE cid
        END as cid,
        CASE WHEN bdate>GETDATE() THEN Null
	         ELSE bdate
        END AS bdate,
        CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	         ELSE 'N/A'
        END AS gen
        from bronze.erm_CUST_AZ12
        SET @end_time = GETDATE();
	    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erm_LOC_A101;
        PRINT('... LOADING DATA IN SILVER.ERM_LOC_A101...')
        INSERT INTO silver.erm_LOC_A101(
        cid, cntry)
        SELECT
        REPLACE(cid, '-','') cid,
        CASE WHEN TRIM(cntry) in ('DE','Germany') THEN 'Germany'
             WHEN TRIM(cntry) in ('US','USA', 'United States') THEN 'United States'
             WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'N/A'
             ELSE TRIM(cntry)
        END AS cntry
        FROM bronze.erm_LOC_A101;
        SET @end_time = GETDATE();
	    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';

        SET @start_time = GETDATE();
        TRUNCATE TABLE silver.erm_PX_CAT_G1V2;
        PRINT('... LOADING DATA IN SILVER.ERM_PX_CAT_G1V2...')
        INSERT INTO silver.erm_PX_CAT_G1V2 (id,cat,subcat,maintenance)

        SELECT 
        id,
        TRIM(cat) as cat,
        TRIM(subcat) as subcat,
        CASE WHEN UPPER(TRIM(maintenance)) in ('Y', 'YES') THEN 'Yes'
             WHEN UPPER(TRIM(maintenance)) in ('N','NO') THEN 'No'
             ELSE 'N/A'
        END AS maintenance
        FROM bronze.erm_PX_CAT_G1V2;
        SET @end_time = GETDATE();
	    PRINT '>> LOAD TIME: ' + cast(DATEDIFF(second,@start_time,@end_time) as nvarchar) + ' seconds.';
        PRINT '####################################################################'
		    SET @batch_end_time = GETDATE();
		    PRINT'>> LOADING FULL SILVER LAYER COMPLETED!'
		    PRINT'>> TIME NEEDED TO LOAD THE FULL SILVER LAYER: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR)+ 'seconds.'
	    END TRY
	    BEGIN CATCH
		    PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		    PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		    PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		    PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	    END CATCH
END
