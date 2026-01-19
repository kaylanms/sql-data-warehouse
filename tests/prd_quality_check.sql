-- =====================================================
-- DATA QUALITY CHECKS 
-- Tabela: bronze.crm_prd_info
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
-- 2. Custos inválidos (negativo ou nulo)
--------------------------------------------------------
SELECT
	*
FROM
	bronze.crm_prd_info
WHERE
	prd_cost < 0
	OR prd_cost IS NULL;

--------------------------------------------------------
-- 3. Domínio de valores - prd_line
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
