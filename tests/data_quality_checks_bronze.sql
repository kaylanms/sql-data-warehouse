/*
====================================================
 DATA QUALITY CHECKS
 Camada: Bronze
 Objetivo: Validação de qualidade dos dados brutos
====================================================

Este script contém validações de qualidade para todas
as tabelas da camada Bronze, incluindo:

✔ Duplicidade
✔ Espaços em branco indevidos
✔ Valores nulos
✔ Domínio de valores
✔ Regras de negócio
✔ Faixa de datas
✔ Consistência entre campos

Uso:
Execute cada bloco conforme necessidade ou rode
o arquivo inteiro para auditoria completa.

====================================================
*/

-- =====================================================
-- TABELA: bronze.crm_cust_info
-- =====================================================

--------------------------------------------------------
-- 1. Duplicidade de clientes
--------------------------------------------------------
SELECT
	cst_id,
	COUNT(*) AS qtd
FROM
	bronze.crm_cust_info
GROUP BY
	cst_id
HAVING
	COUNT(*) > 1;

--------------------------------------------------------
-- 2. Espaços indevidos - Primeiro nome
--------------------------------------------------------
SELECT
	cst_firstname
FROM
	bronze.crm_cust_info
WHERE
	TRIM(cst_firstname) <> cst_firstname;

--------------------------------------------------------
-- 3. Espaços indevidos - Sobrenome
--------------------------------------------------------
SELECT
	cst_lastname
FROM
	bronze.crm_cust_info
WHERE
	TRIM(cst_lastname) <> cst_lastname;

--------------------------------------------------------
-- 4. Gênero nulo
--------------------------------------------------------
SELECT
	*
FROM
	bronze.crm_cust_info
WHERE
	cst_gndr IS NULL;

-- =====================================================
-- TABELA: bronze.erp_cust_az12
-- =====================================================

--------------------------------------------------------
-- 1. Validação de faixa - DATA DE NASCIMENTO
--------------------------------------------------------
-- Regras:
-- Data >= 1924-01-01
-- Data <= data atual
--------------------------------------------------------
SELECT
    bdate
FROM
    bronze.erp_cust_az12
WHERE
    bdate < DATE '1924-01-01'
    OR bdate > CURRENT_DATE;

--------------------------------------------------------
-- 2. Validação de domínio - GÊNERO
--------------------------------------------------------
SELECT DISTINCT
    gen
FROM
    bronze.erp_cust_az12
ORDER BY
    gen;

-- =====================================================
-- TABELA: bronze.erp_loc_a101
-- =====================================================

--------------------------------------------------------
-- 1. Validação de domínio - PAÍS
--------------------------------------------------------
SELECT DISTINCT
    cntry
FROM
    bronze.erp_loc_a101
ORDER BY
    cntry;

-- =====================================================
-- TABELA: bronze.erp_px_cat_g1v2
-- =====================================================

--------------------------------------------------------
-- 1. Validação de espaços em branco (TRIM)
--------------------------------------------------------
SELECT
    *
FROM
    bronze.erp_px_cat_g1v2
WHERE
    cat <> TRIM(cat)
    OR subcat <> TRIM(subcat)
    OR maintenance <> TRIM(maintenance);

--------------------------------------------------------
-- 2. Validação de domínio - CATEGORIA
--------------------------------------------------------
SELECT DISTINCT
    cat
FROM
    bronze.erp_px_cat_g1v2
ORDER BY
    cat;

--------------------------------------------------------
-- 3. Validação de domínio - SUBCATEGORIA
--------------------------------------------------------
SELECT DISTINCT
    subcat
FROM
    bronze.erp_px_cat_g1v2
ORDER BY
    subcat;

--------------------------------------------------------
-- 4. Validação de domínio - MANUTENÇÃO
--------------------------------------------------------
SELECT DISTINCT
    maintenance
FROM
    bronze.erp_px_cat_g1v2
ORDER BY
    maintenance;

-- =====================================================
-- TABELA: bronze.crm_prd_info
-- =====================================================

--------------------------------------------------------
-- 1. Duplicidade de produto
--------------------------------------------------------
SELECT
	prd_id,
	COUNT(*) AS qtd
FROM
	bronze.crm_prd_info
GROUP BY
	prd_id
HAVING
	COUNT(*) > 1;

--------------------------------------------------------
-- 2. Custos inválidos
--------------------------------------------------------
SELECT
	*
FROM
	bronze.crm_prd_info
WHERE
	prd_cost < 0
	OR prd_cost IS NULL;

--------------------------------------------------------
-- 3. Validação de domínio - prd_line
--------------------------------------------------------
SELECT DISTINCT
	prd_line
FROM
	bronze.crm_prd_info
ORDER BY
	prd_line;

--------------------------------------------------------
-- 4. Validação de datas
--------------------------------------------------------
SELECT
	*
FROM
	bronze.crm_prd_info
WHERE
	prd_end_dt < prd_start_dt;

-- =====================================================
-- TABELA: bronze.crm_sales_details
-- =====================================================

--------------------------------------------------------
-- 1. Validação de formato - ORDER DATE
--------------------------------------------------------
SELECT
    sls_order_dt
FROM
    bronze.crm_sales_details
WHERE
    LENGTH(sls_order_dt::TEXT) <> 8
    OR sls_order_dt > 20500101
    OR sls_order_dt < 19000101;

--------------------------------------------------------
-- 2. Validação de formato - SHIP DATE
--------------------------------------------------------
SELECT
    sls_ship_dt
FROM
    bronze.crm_sales_details
WHERE
    LENGTH(sls_ship_dt::TEXT) <> 8
    OR sls_ship_dt > 20500101
    OR sls_ship_dt < 19000101;

--------------------------------------------------------
-- 3. Validação de formato - DUE DATE
--------------------------------------------------------
SELECT
    sls_due_dt
FROM
    bronze.crm_sales_details
WHERE
    LENGTH(sls_due_dt::TEXT) <> 8
    OR sls_due_dt > 20500101
    OR sls_due_dt < 19000101;

--------------------------------------------------------
-- 4. Regras de negócio entre datas
--------------------------------------------------------
SELECT
    *
FROM
    bronze.crm_sales_details
WHERE
    sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt
    OR sls_ship_dt > sls_due_dt;

--------------------------------------------------------
-- 5. Validação de consistência: SALES x QUANTITY x PRICE
--------------------------------------------------------
SELECT DISTINCT
    CASE
        WHEN sls_sales IS NULL
             OR sls_sales <= 0
             OR sls_sales <> sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,

    sls_quantity,

    CASE
        WHEN sls_price IS NULL
             OR sls_price <= 0
        THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END AS sls_price

FROM
    bronze.crm_sales_details
WHERE
    sls_sales <> sls_quantity * sls_price
    OR sls_sales IS NULL
    OR sls_quantity IS NULL
    OR sls_price IS NULL
    OR sls_sales <= 0
    OR sls_quantity <= 0
    OR sls_price <= 0
ORDER BY
    sls_quantity;


-- =====================================================
-- FIM DO SCRIPT
-- =====================================================
