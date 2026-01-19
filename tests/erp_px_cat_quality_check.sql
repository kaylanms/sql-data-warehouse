-- =====================================================
-- DATA QUALITY CHECKS
-- Tabela: bronze.erp_px_cat_g1v2
-- =====================================================

--------------------------------------------------------
-- 1. Validação de espaços em branco (TRIM)
--------------------------------------------------------
-- Identifica registros com espaços antes/depois do valor
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
