/* ============================================================
   PROCEDURE NAME : bronze.load_bronze

   PURPOSE:
       Performs a FULL LOAD of all Bronze layer tables
       using a truncate-and-reload strategy.

   LOAD STRATEGY:
       1. Truncate existing table data
       2. Reload fresh data using BULK INSERT
       3. Log execution progress using PRINT statements
       4. Track load duration per table
       5. Track total batch duration
       6. Handle runtime errors using TRY...CATCH

   NEW ENHANCEMENTS ADDED:
       ✔ Table-level load duration tracking
       ✔ Batch-level load duration tracking
       ✔ Structured progress logging
       ✔ Error handling using TRY...CATCH
       ✔ Error message reporting using ERROR_MESSAGE()
       ✔ Execution timestamps using GETDATE()
       ✔ Time calculation using DATEDIFF()

   ERROR HANDLING:
       - Captures runtime errors
       - Displays error message, state, and severity
       - Prevents silent failures

   EXECUTION:
       EXEC bronze.load_bronze;

   LAYER DESCRIPTION:
       Bronze Layer = Raw data ingestion layer
       Data is loaded exactly as received from source systems

============================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY

        SET @batch_start_time = GETDATE();

        PRINT '========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '========================================';

        ----------------------------------------------------------
        -- CRM TABLES
        ----------------------------------------------------------

        PRINT '----------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- CRM CUSTOMER INFORMATION
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- CRM PRODUCT INFORMATION
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- CRM SALES DETAILS
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- ERP TABLES
        ----------------------------------------------------------

        PRINT '----------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- ERP CUSTOMER ADDITIONAL DATA
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- ERP LOCATION DATA
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- ERP PRODUCT CATEGORY DATA
        ----------------------------------------------------------
        SET @start_time = GETDATE();

        PRINT '>>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '----------------------------------------';

        ----------------------------------------------------------
        -- BATCH COMPLETION
        ----------------------------------------------------------
        SET @batch_end_time = GETDATE();

        PRINT '========================================';
        PRINT 'Loading Bronze Layer Completed Successfully';
        PRINT 'Total Load Duration: '
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(20))
              + ' seconds';
        PRINT '========================================';

    END TRY

    BEGIN CATCH

        PRINT '========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Message  : ' + ERROR_MESSAGE();
        PRINT 'Error Severity : ' + CAST(ERROR_SEVERITY() AS NVARCHAR(10));
        PRINT 'Error State    : ' + CAST(ERROR_STATE() AS NVARCHAR(10));
        PRINT '========================================';

    END CATCH

END;
GO
