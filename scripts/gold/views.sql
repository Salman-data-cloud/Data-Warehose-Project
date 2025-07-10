/* Here I created the fact and dimention views based on the insights I got while doing the project*/

CREATE VIEW gold.dim_customers AS

SELECT
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS cunstomer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ca.bdate AS birthdate,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	     ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_marital_status AS marital_status,
	
	ci.cst_create_date AS create_date
	
	
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erm_CUST_AZ12 ca 
	ON ci.cst_key = ca.cid
	LEFT JOIN silver.erm_LOC_A101 la
	ON ci.cst_key = la.cid
Go

CREATE VIEW gold.dim_products AS 
SELECT
ROW_NUMBER() OVER (ORDER BY pn.prd_id, pn.prd_start_dt) AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.category_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pn.prd_cost AS product_cost,
pn.prd_line AS delivary_zone,
pn.prd_start_dt AS product_start_date,
pc.maintenance AS need_of_maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erm_PX_CAT_G1V2 pc
ON pn.category_id = pc.id
WHERE prd_end_dt is null -- FILTER OUT ALL HISTORICAL DATA
Go

CREATE VIEW gold.fact_sales AS
SELECT 
sd.sls_ord_num AS order_number,
pr.product_key,
cs.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity as quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cs
ON sd.sls_cust_id = cs.customer_id








