/* ============================================================
   SILVER LAYER - TABLE CREATION SCRIPT
   Purpose  : 
       - Drop existing Silver tables (if they exist)
       - Recreate Silver tables with clean structure
       - Prepare tables for transformed data load from Bronze layer
   ============================================================ */


/* ============================================================
   TABLE: silver.crm_cust_info
   Description:
       Stores cleaned customer master data from CRM source.
       Data is transformed and validated before loading here.
   ============================================================ */

IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,                 -- Unique Customer ID
    cst_key            NVARCHAR(50),        -- Business customer key
    cst_firstname      NVARCHAR(50),        -- Customer first name (trimmed & cleaned)
    cst_lastname       NVARCHAR(50),        -- Customer last name (trimmed & cleaned)
    cst_marital_status NVARCHAR(50),        -- Marital status (standardized values)
    cst_gndr           NVARCHAR(50),        -- Gender (standardized values)
    cst_create_date    DATE,                -- Customer creation date in source system
    dwh_create_date    DATETIME2 DEFAULT GETDATE() -- Data warehouse load timestamp
);
GO


/* ============================================================
   TABLE: silver.crm_prd_info
   Description:
       Stores cleaned product master data from CRM source.
   ============================================================ */

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,                 -- Unique Product ID
    cat_id          NVARCHAR(50),        -- Category ID
    prd_key         NVARCHAR(50),        -- Business product key
    prd_nm          NVARCHAR(50),        -- Product name (cleaned)
    prd_cost        INT,                 -- Product cost
    prd_line        NVARCHAR(50),        -- Product line classification
    prd_start_dt    DATE,                -- Product start date
    prd_end_dt      DATE,                -- Product end date
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Load timestamp
);
GO


/* ============================================================
   TABLE: silver.crm_sales_details
   Description:
       Stores cleaned transactional sales data.
       Represents order-level sales details.
   ============================================================ */

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),        -- Sales order number
    sls_prd_key     NVARCHAR(50),        -- Product key
    sls_cust_id     INT,                 -- Customer ID (FK reference)
    sls_order_dt    DATE,                -- Order date
    sls_ship_dt     DATE,                -- Shipping date
    sls_due_dt      DATE,                -- Due date
    sls_sales       INT,                 -- Total sales amount
    sls_quantity    INT,                 -- Quantity sold
    sls_price       INT,                 -- Unit price
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Load timestamp
);
GO


/* ============================================================
   TABLE: silver.erp_loc_a101
   Description:
       Stores cleaned ERP location data.
   ============================================================ */

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),        -- Customer ID (ERP reference)
    cntry           NVARCHAR(50),        -- Country name (standardized)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Load timestamp
);
GO


/* ============================================================
   TABLE: silver.erp_cust_az12
   Description:
       Stores cleaned ERP customer demographic data.
   ============================================================ */

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),        -- Customer ID (ERP reference)
    bdate           DATE,                -- Birth date
    gen             NVARCHAR(50),        -- Gender (standardized)
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Load timestamp
);
GO


/* ============================================================
   TABLE: silver.erp_px_cat_g1v2
   Description:
       Stores cleaned ERP product category hierarchy data.
   ============================================================ */

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),        -- Product ID
    cat             NVARCHAR(50),        -- Main category
    subcat          NVARCHAR(50),        -- Sub-category
    maintenance     NVARCHAR(50),        -- Maintenance type
    dwh_create_date DATETIME2 DEFAULT GETDATE() -- Load timestamp
);
GO
