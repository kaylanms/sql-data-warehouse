/*
====================================================
  CAMADA GOLD - DATA WAREHOUSE

  Criação das views dimensionais e fato da camada GOLD.
  Esta camada representa os dados prontos para consumo
  analítico (BI, dashboards, relatórios e análises).

  Views criadas:
  - gold.dim_customers  -> Dimensão de clientes
  - gold.dim_products   -> Dimensão de produtos
  - gold.fact_sales     -> Fato de vendas

  Objetivos:
  - Modelagem dimensional (Star Schema)
  - Geração de chaves substitutas (surrogate keys)
  - Padronização de atributos
  - Integração entre fontes CRM e ERP
  - Dados prontos para análise

  Pré-requisitos:
  - Camada SILVER carregada
  - Procedures executadas:
      CALL silver.load_silver();
====================================================
*/


----------------------------------------------------
-- DIMENSÃO: CLIENTES
----------------------------------------------------
/*
  View: gold.dim_customers

  Descrição:
  Dimensão de clientes consolidando dados do CRM
  e informações complementares do ERP.

  Fontes:
  - silver.crm_cust_info
  - silver.erp_cust_az12
  - silver.erp_loc_a101

  Regras:
  - Geração de surrogate key (customer_key)
  - Prioriza gênero do CRM
  - Caso CRM = 'N/A', busca no ERP
  - Padronização de país
*/
CREATE OR REPLACE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY ci.cst_id
    ) AS customer_key,            -- Chave substituta

    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.cntry         AS country,
    ci.cst_marital_status AS marital_status,

    -- Regra de priorização de gênero
    CASE
        WHEN ci.cst_gndr <> 'N/A' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'N/A')
    END AS gender,

    ca.bdate          AS birthdate,
    ci.cst_create_date AS create_date

FROM silver.crm_cust_info ci

LEFT JOIN silver.erp_cust_az12 ca
       ON ci.cst_key = ca.cid

LEFT JOIN silver.erp_loc_a101 la
       ON ci.cst_key = la.cid;



----------------------------------------------------
-- DIMENSÃO: PRODUTOS
----------------------------------------------------
/*
  View: gold.dim_products

  Descrição:
  Dimensão de produtos com categorização
  proveniente do ERP.

  Fontes:
  - silver.crm_prd_info
  - silver.erp_px_cat_g1v2

  Regras:
  - Considera apenas produtos ativos
  - prd_end_dt IS NULL
  - Geração de surrogate key (product_key)
*/
CREATE OR REPLACE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (
        ORDER BY
            pn.prd_start_dt,
            pn.prd_key
    ) AS product_key,            -- Chave substituta

    pn.prd_id   AS product_id,
    pn.prd_key  AS product_number,
    pn.prd_nm   AS product_name,
    pn.cat_id   AS category_id,

    pc.cat      AS category,
    pc.subcat   AS subcategory,
    pc.maintenance,

    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn

LEFT JOIN silver.erp_px_cat_g1v2 pc
       ON pn.cat_id = pc.id

-- Apenas produtos vigentes
WHERE pn.prd_end_dt IS NULL;



----------------------------------------------------
-- FATO: VENDAS
----------------------------------------------------
/*
  View: gold.fact_sales

  Descrição:
  Tabela fato de vendas, relacionando
  clientes e produtos.

  Granularidade:
  - 1 linha = 1 item de pedido

  Fontes:
  - silver.crm_sales_details
  - gold.dim_products
  - gold.dim_customers

  Métricas:
  - sales_amount
  - quantity
  - price
*/
CREATE OR REPLACE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num AS order_number,

    pr.product_key,
    cu.customer_key,

    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price

FROM silver.crm_sales_details sd

LEFT JOIN gold.dim_products pr
       ON pr.product_number = sd.sls_prd_key

LEFT JOIN gold.dim_customers cu
       ON cu.customer_id = sd.sls_cust_id

ORDER BY
    cu.customer_key,
    pr.product_key;
