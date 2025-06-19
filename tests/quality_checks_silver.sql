/*
========================================================================================================
Quality Checks
========================================================================================================
Script Purpose:
The script performs various quality checks from the data consistency,accuracy and standardization across 'silver'schema. Includes checks for:
-Null od duplicate primary keys.
-Unwanted spaces in String fields.
-Data standardization and consistency.
-Invalid data ranges and orders.
-Data consistency between related fields.
==========================================================================================================
*/



--CHECK for NULL or Duplicates
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL


--DATA STANDARDIZATION & CONSISTENCY
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT * FROM silver.crm_cust_info

SELECT cst_key
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key) 

----check unwanted spaces
SELECT cst_lastname
FROM silver.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)

SELECT * FROM silver.crm_cust_info

  SELECT 
  cst_id,
  cst_key,
  TRIM(cst_firstname) AS cst_firstname,
  TRIM(cst_lastname) AS cst_lastname,   ----Remove unnecessary spaces ,unwanted characters

CASE 
     WHEN UPPER(TRIM(cst_marital_status))= 'S' THEN 'Single'---------Data Normalization/Standardization
     WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
     ELSE 'n/a'-------------------------------------------------------handling missing values
END AS cst_marital_status,

CASE 
     WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
     WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
     ELSE 'n/a'
END AS cst_gndr,
cst_create_date
FROM(
SELECT * ,
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last =1 

  

---CHECK for NULL Values
SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL

--CHECK unwanted Spaces
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

--CHECK NULL or Negative NUM
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--DATA STANDARDIZATION and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

--CHECK for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT *
FROM silver.crm_prd_info

 --CHECK invalid Dates
SELECT 
NULLIF(sls_due_dt,0) sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_due_dt <=0 
OR LEN(sls_due_dt) !=8
OR sls_due_dt > 20500101
OR sls_due_dt < 19000101

SELECT * FROM silver.crm_sales_details
WHERE sls_order_dt> sls_ship_dt OR sls_order_dt > sls_due_dt

--CHECK for the squence of the dates for all 3 columns 
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

--CHECK Consistency of DATA
--Sales = Quantity* Price
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price   
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_price IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price

-----
--Identify Out-of-Range

SELECT DISTINCT
bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

--DATA STANDARDIZATION 
SELECT DISTINCT gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
      ELSE 'n/a'
END AS gen
FROM silver.erp_cust_az12

SELECT* FROM silver.erp_cust_az12
END AS gen
FROM bronze.erp_cust_az12

--DATA Standardization
SELECT DISTINCT cntry 
FROM silver.erp_loc_a101
ORDER BY cntry

SELECT * FROM  silver.erp_loc_a101
-------------------------------
  
INSERT INTO silver.erp_px_cat_g1v2
(id,cat,subcat, maintenance)

SELECT
id,
cat,
subcat,
maintenance 
FROM bronze.erp_px_cat_g1v2

--CHECK for unwanted spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat ! = TRIM(cat) OR subcat ! = TRIM(subcat) OR maintenance != TRIM(maintenance)

--DATA Standardization and Consistency
SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2


