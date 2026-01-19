-- =====================================================
-- DATA QUALITY CHECKS
-- Tabela: bronze.erp_loc_a101
-- =====================================================

--------------------------------------------------------
-- 1. Validação de domínio - PAÍS (CNTRY)
--------------------------------------------------------
-- Identificar todos os valores distintos existentes
-- para posterior padronização (ISO, abreviações, etc)
--------------------------------------------------------
SELECT DISTINCT
    cntry
FROM
    bronze.erp_loc_a101;
