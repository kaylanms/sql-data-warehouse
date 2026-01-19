-- =====================================================
-- DATA QUALITY CHECKS
-- Tabela: bronze.crm_sales_details
-- =====================================================

--------------------------------------------------------
-- 1. Validação de formato e faixa - ORDER DATE
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
-- 2. Validação de formato e faixa - SHIP DATE
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
-- 3. Validação de formato e faixa - DUE DATE
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
-- Regras:
-- Order Date não pode ser maior que Ship Date
-- Order Date não pode ser maior que Due Date
-- Ship Date não pode ser maior que Due Date
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
-- 5. Validação de consistência: SALES, QUANTITY e PRICE
--------------------------------------------------------
-- Regras:
-- sales = quantity * price
-- Valores não podem ser NULL, zero ou negativos
--
-- Tratamentos:
-- - Se sales for NULL, <= 0 ou inconsistente → recalcula
-- - Se price for NULL ou <= 0 → recalcula
-- - Se price for negativo → converte para positivo
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
