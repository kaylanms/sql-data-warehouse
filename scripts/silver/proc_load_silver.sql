/*
====================================================
  STORED PROCEDURE: silver.load_silver
  Database: data_warehouse
  Camada: Silver (layer refinada)
  
  Descrição:
  Esta procedure executa o processo de FULL LOAD da
  camada Bronze para a camada Silver do Data Warehouse.

  Principais responsabilidades:
  - Limpar dados (TRIM, padronização de textos)
  - Aplicar regras de negócio
  - Normalizar domínios (gênero, estado civil, países, etc.)
  - Tratar valores inválidos ou nulos
  - Converter e validar campos de data
  - Garantir versionamento correto (registros mais recentes)
  - Calcular métricas (sales = quantity * price)
  - Padronizar chaves e formatos

  Funcionalidades implementadas:
  - TRUNCATE antes da carga (FULL LOAD)
  - Logs detalhados por tabela
  - Contagem de linhas inseridas
  - Medição de tempo por etapa
  - Medição de tempo total
  - Tratamento de erros individual por tabela
  - Execução independente por domínio (CRM / ERP)

  Uso:
  CALL silver.load_silver();

  Observação:
  Deve ser executada após a conclusão do load da
  camada Bronze (procedure: bronze.load_bronze)
====================================================
*/


CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows      INT;
    step_start  TIMESTAMP;
    step_end    TIMESTAMP;
    total_start TIMESTAMP;
    total_end   TIMESTAMP;
BEGIN

--------------------------------------------------------
-- START
--------------------------------------------------------
RAISE NOTICE '===========================================';
RAISE NOTICE ' INICIANDO CARGA - CAMADA SILVER ';
RAISE NOTICE '===========================================';

total_start := clock_timestamp();

--------------------------------------------------------
-- CRM CUSTOMER
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.crm_cust_info';
    step_start := clock_timestamp();

    TRUNCATE TABLE silver.crm_cust_info;

    INSERT INTO silver.crm_cust_info (
        cst_id, cst_key, cst_firstname, cst_lastname,
        cst_marital_status, cst_gndr, cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname),
        TRIM(cst_lastname),
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'S' THEN 'Single'
            WHEN 'M' THEN 'Married'
            ELSE 'N/A'
        END,
        CASE UPPER(TRIM(cst_gndr))
            WHEN 'F' THEN 'Female'
            WHEN 'M' THEN 'Male'
            ELSE 'N/A'
        END,
        cst_create_date
    FROM (
        SELECT *,
               ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) s
    WHERE flag_last = 1;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.crm_cust_info -> %', SQLERRM;
END;

--------------------------------------------------------
-- CRM PRODUCT
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.crm_prd_info';
    step_start := clock_timestamp();

    INSERT INTO silver.crm_prd_info (
        prd_id, cat_id, prd_key, prd_nm,
        prd_cost, prd_line, prd_start_dt, prd_end_dt
    )
    SELECT
        prd_id,
        REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
        SUBSTRING(prd_key,7),
        prd_nm,
        COALESCE(prd_cost,0),
        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END,
        prd_start_dt::DATE,
        CAST(
            LEAD(prd_start_dt) OVER(
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - INTERVAL '1 day' AS DATE
        )
    FROM bronze.crm_prd_info;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.crm_prd_info -> %', SQLERRM;
END;

--------------------------------------------------------
-- CRM SALES
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.crm_sales_details';
    step_start := clock_timestamp();

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id,
        sls_order_dt, sls_ship_dt, sls_due_dt,
        sls_sales, sls_quantity, sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        CASE
            WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) <> 8 THEN NULL
            ELSE sls_order_dt::TEXT::DATE
        END,
        sls_ship_dt::TEXT::DATE,
        sls_due_dt::TEXT::DATE,
        CASE
            WHEN sls_sales IS NULL
              OR sls_sales <= 0
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END,
        sls_quantity,
        CASE
            WHEN sls_price IS NULL OR sls_price <= 0
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
        END
    FROM bronze.crm_sales_details;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.crm_sales_details -> %', SQLERRM;
END;

--------------------------------------------------------
-- ERP CUSTOMER
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.erp_cust_az12';
    step_start := clock_timestamp();

    INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4)
            ELSE cid
        END,
        CASE
            WHEN bdate > CURRENT_DATE THEN NULL
            ELSE bdate
        END,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
            ELSE 'N/A'
        END
    FROM bronze.erp_cust_az12;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.erp_cust_az12 -> %', SQLERRM;
END;

--------------------------------------------------------
-- ERP LOCATION
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.erp_loc_a101';
    step_start := clock_timestamp();

    INSERT INTO silver.erp_loc_a101 (cid,cntry)
    SELECT
        REPLACE(cid,'-',''),
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
            WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
            ELSE TRIM(cntry)
        END
    FROM bronze.erp_loc_a101;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.erp_loc_a101 -> %', SQLERRM;
END;

--------------------------------------------------------
-- ERP CATEGORY
--------------------------------------------------------
BEGIN
    RAISE NOTICE '-> Iniciando: silver.erp_px_cat_g1v2';
    step_start := clock_timestamp();

    INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
    SELECT id,cat,subcat,maintenance
    FROM bronze.erp_px_cat_g1v2;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    step_end := clock_timestamp();

    RAISE NOTICE '   Linhas inseridas: %', v_rows;
    RAISE NOTICE '   Tempo: % segundos',
        EXTRACT(EPOCH FROM step_end - step_start);

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '   ERRO silver.erp_px_cat_g1v2 -> %', SQLERRM;
END;

--------------------------------------------------------
-- FINAL
--------------------------------------------------------
total_end := clock_timestamp();

RAISE NOTICE '===========================================';
RAISE NOTICE ' LOAD FINALIZADO COM SUCESSO ';
RAISE NOTICE ' Tempo total: % segundos ',
    EXTRACT(EPOCH FROM total_end - total_start);
RAISE NOTICE '===========================================';

END;
$$;

-- EXECUÇÃO
CALL silver.load_silver();
