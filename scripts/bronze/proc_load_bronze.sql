/*
====================================================
 STORED PROCEDURE: bronze.load_bronze
 Database: data_warehouse
 Camada: Bronze (camada bruta)
 Tipo de carga: FULL LOAD

 Descrição:
 Esta stored procedure é responsável pela ingestão
 completa dos dados brutos (raw data) para a camada
 Bronze do Data Warehouse.

 Funcionalidades:
 - Executa TRUNCATE nas tabelas antes da carga
 - Realiza ingestão via COPY a partir de arquivos CSV
 - Registra logs detalhados por tabela
 - Mede tempo de execução individual de cada carga
 - Conta quantidade de linhas carregadas
 - Mede tempo total de execução do processo
 - Possui tratamento de erros por etapa (TRY/CATCH)
 - Permite continuidade mesmo se uma tabela falhar

 Objetivo da camada Bronze:
 - Armazenar dados brutos sem transformação
 - Garantir rastreabilidade da origem dos dados
 - Servir como base para a camada Silver
 - Manter integridade do dado original

 Fonte de dados:
 - CRM (cust_info, prd_info, sales_details)
 - ERP (cust_az12, loc_a101, px_cat_g1v2)

 Uso:
 CALL bronze.load_bronze();

 Observações:
 - Deve ser executada antes da procedure:
   silver.load_silver()
 - Os caminhos dos arquivos CSV devem ser
   ajustados conforme o ambiente

====================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    total_start TIMESTAMP;
    total_end TIMESTAMP;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count BIGINT;
BEGIN

    ------------------------------------------------------------------------
    -- INÍCIO DO LOAD
    ------------------------------------------------------------------------
    RAISE NOTICE '==============================================';
    RAISE NOTICE '=== Iniciando carga da camada bronze ===';
    RAISE NOTICE '==============================================';
    total_start := clock_timestamp();

    ------------------------------------------------------------------------
    -- CRM - CUSTOMER
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.crm_cust_info';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '<CAMINHO_DO_ARQUIVO>/cust_info.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.crm_cust_info;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.crm_cust_info | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.crm_cust_info: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- CRM - PRODUCT
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.crm_prd_info';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.crm_prd_info;
        COPY bronze.crm_prd_info
        FROM '<CAMINHO_DO_ARQUIVO>/prd_info.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.crm_prd_info;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.crm_prd_info | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.crm_prd_info: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- CRM - SALES
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.crm_sales_details';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '<CAMINHO_DO_ARQUIVO>/sales_details.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.crm_sales_details;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.crm_sales_details | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.crm_sales_details: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- ERP - CUSTOMER
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.erp_cust_az12';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '<CAMINHO_DO_ARQUIVO>/CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.erp_cust_az12;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.erp_cust_az12 | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.erp_cust_az12: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- ERP - LOCATION
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.erp_loc_a101';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '<CAMINHO_DO_ARQUIVO>/LOC_A101.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.erp_loc_a101;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.erp_loc_a101 | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.erp_loc_a101: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- ERP - CATEGORY
    ------------------------------------------------------------------------
    BEGIN
        RAISE NOTICE '==============================';
        RAISE NOTICE 'Iniciando carga: bronze.erp_px_cat_g1v2';
        RAISE NOTICE '==============================';
        start_time := clock_timestamp();

        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '<CAMINHO_DO_ARQUIVO>/PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER true);

        SELECT COUNT(*) INTO row_count FROM bronze.erp_px_cat_g1v2;

        end_time := clock_timestamp();
        RAISE NOTICE 'Carga concluída: bronze.erp_px_cat_g1v2 | Duração: % segundos | Linhas carregadas: %', 
                     EXTRACT(EPOCH FROM end_time - start_time), row_count;
        RAISE NOTICE '------------------------------';
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Falha na tabela bronze.erp_px_cat_g1v2: %', SQLERRM;
    END;

    ------------------------------------------------------------------------
    -- FINAL
    ------------------------------------------------------------------------
    total_end := clock_timestamp();
    RAISE NOTICE '==============================================';
    RAISE NOTICE '=== Carga da camada bronze finalizada ===';
    RAISE NOTICE '=== Tempo total: % segundos ===', EXTRACT(EPOCH FROM total_end - total_start);
    RAISE NOTICE '==============================================';

END;
$$;

-- ========================================
-- EXECUÇÃO
-- ========================================
CALL bronze.load_bronze();
