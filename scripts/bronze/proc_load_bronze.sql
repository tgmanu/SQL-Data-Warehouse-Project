/* ============================================================
   PROCEDURE NAME : bronze.load_bronze

   PURPOSE:
       This stored procedure performs a FULL LOAD of all
       Bronze layer tables in the Data Warehouse.

       It follows a truncate-and-reload strategy:
           1. Removes existing data using TRUNCATE TABLE
           2. Reloads fresh data using BULK INSERT from CSV files

   WHY STORED PROCEDURE IS USED:
       - Encapsulates ETL logic in a reusable database object
       - Improves maintainability and code organization
       - Allows execution using a single command (EXEC)
       - Simplifies automation via SQL Server Agent or schedulers

   CREATE OR ALTER:
       - Creates the procedure if it does not exist
       - Modifies the procedure if it already exists
       - Eliminates the need to manually drop and recreate

   SCHEMA:
       - The procedure is created under the 'bronze' schema
       - Naming convention aligns with layered architecture design

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

    ----------------------------------------------------------
    -- CRM CUSTOMER INFORMATION
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\cust_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- CRM PRODUCT INFORMATION
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\prd_info.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- CRM SALES DETAILS
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_crm\sales_details.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- ERP CUSTOMER ADDITIONAL DATA
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\CUST_AZ12.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- ERP LOCATION DATA
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\LOC_A101.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    ----------------------------------------------------------
    -- ERP PRODUCT CATEGORY DATA
    ----------------------------------------------------------
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\manut\Downloads\sql-data-warehouse-system-2025\sql-data-warehouse-project-2025\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH (
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

END;
GO
