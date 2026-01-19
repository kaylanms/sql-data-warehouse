-- =====================================================
-- DATA QUALITY CHECKS
-- Tabela: bronze.crm_cust_info
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
