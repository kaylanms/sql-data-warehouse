-- =====================================================
-- DATA QUALITY CHECKS
-- Tabela: bronze.erp_cust_az12
-- Objetivo: Validar datas de nascimento e domínio de gênero
-- =====================================================

--------------------------------------------------------
-- 1. Validação de faixa - DATA DE NASCIMENTO (BDATE)
--------------------------------------------------------
-- Regras:
-- Data não pode ser anterior a 1924-01-01
-- Data não pode ser maior que a data atual
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
-- Identificar valores distintos existentes no campo
--------------------------------------------------------
SELECT DISTINCT
    gen
FROM
    bronze.erp_cust_az12;
