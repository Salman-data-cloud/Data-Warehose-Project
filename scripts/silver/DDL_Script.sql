/* This is the DDL Script for the silver layer of the Data Warehouse*/

use DataWarehouse;
Go
IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_cust_info;
create table silver.crm_cust_info(
cst_id INT,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(20),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
category_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity int,
sls_price int,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);

Go
IF OBJECT_ID('silver.erm_CUST_AZ12', 'U') IS NOT NULL
	DROP TABLE silver.erm_CUST_AZ12;
create table silver.erm_CUST_AZ12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
IF OBJECT_ID('silver.erm_LOC_A101', 'U') IS NOT NULL
	DROP TABLE silver.erm_LOC_A101;
create table silver.erm_LOC_A101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
Go
IF OBJECT_ID('silver.erm_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE silver.erm_PX_CAT_G1V2;
create table silver.erm_PX_CAT_G1V2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(20),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
