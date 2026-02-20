/* ============================================================
   PROCEDURE NAME : bronze.load_bronze

   PURPOSE:
       Performs a FULL LOAD of all Bronze layer tables
       using a truncate-and-reload strategy.

       Steps:
           1. Removes existing data using TRUNCATE TABLE
           2. Reloads fresh data from CSV files using BULK INSERT
           3. Displays execution progress using PRINT statements

   PRINT STATEMENTS:
       - Display execution status in the Messages tab
       - Help track which table group is loading
       - Useful for debugging and monitoring ETL runs

   WHY STORED PROCEDURE:
       - Encapsulates ETL logic into a reusable database object
       - Enables execution with a single command
       - Improves maintainability and organization
       - Supports scheduling via SQL Server Agent

   CREATE OR ALTER:
       - Creates the procedure if it does not exist
       - Updates it if it already exists

   EXECUTION:
       EXEC bronze.load_bronze;

   LAYER STRATEGY:
       Bronze Layer = Raw data ingestion layer
       Data is loaded exactly as received from source systems
============================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN

    SET NOCOUNT ON;

    PRINT '========================================';
    PRINT 'Loading Bronze Layer';
    PRINT '========================================';

    ----------------------------------------------------------
    -- CRM TABLES
    ----------------------------------------------------------

    PRINT '----------------------------------------';
    PRINT 'Loading CRM Tables';
    PRINT '----------------------------------------';

    -- CRM CUSTOMER INFORMATION
    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- CRM PRODUCT INFORMATION
    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- CRM SALES DETAILS
    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- ERP TABLES
    ----------------------------------------------------------

    PRINT '----------------------------------------';
    PRINT 'Loading ERP Tables';
    PRINT '----------------------------------------';

    -- ERP CUSTOMER ADDITIONAL DATA
    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- ERP LOCATION DATA
    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    -- ERP PRODUCT CATEGORY DATA
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    PRINT '========================================';
    PRINT 'Bronze Layer Load Completed Successfully';
    PRINT '========================================';

END;
GO
