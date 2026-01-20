# | Data Warehouse

Este projeto demonstra a implementação de uma arquitetura completa de Data Warehouse, abordando desde a ingestão e processamento dos dados até a modelagem e organização das camadas. A solução foi estruturada seguindo boas práticas de engenharia de dados, com foco em performance, escalabilidade e qualidade das informações.

# > Arquitetura de dados

![Data Architecture](docs/data_architecture.gif)

**Bronze Layer**: armazena dados brutos, sem transformações, replica exatamente o formato da origem, carga via Stored Procedure. Estratégia: TRUNCATE + INSERT

**Silver Layer**: dados tratados e padronizados, aplicação de regras de negócio, enriquecimento de dados, normalização de domínios, carga via Stored Procedure.

**Gold Layer**: dados prontos para consumo, views analíticas, modelo dimensional (Star Schema), utilizado por BI, SQL e ML.

# > Modelo de dados

![Modelo de dados](docs/data_model.gif)

# > Gold Layer – Data Catalog

A **Gold Layer** representa a camada de dados voltada ao negócio, estruturada para suportar análises e relatórios. Nela, os dados já estão **tratados, modelados e organizados** em tabelas fato e dimensões, seguindo os princípios de **modelagem dimensional**.

O modelo é composto pelas seguintes tabelas principais:

### `dim_customers`

Armazena informações detalhadas dos clientes, enriquecidas com dados demográficos e geográficos. Inclui identificadores técnicos (surrogate keys), dados pessoais, país de residência, status civil, gênero, data de nascimento e data de criação do registro. Essa dimensão permite análises segmentadas por perfil de cliente. 

---

### `dim_products`

Contém os atributos dos produtos, como identificadores internos, código do produto, nome, categoria, subcategoria, linha do produto, custo e indicação de necessidade de manutenção. Essa tabela permite análises por tipo de produto, categoria e ciclo de vida.

---

### `fact_sales`

Tabela fato responsável por armazenar os dados transacionais de vendas. Possui relacionamento com as dimensões de clientes e produtos por meio de chaves substitutas. Registra informações como número do pedido, datas (pedido, envio e vencimento), quantidade vendida, preço unitário e valor total da venda.

Essa estrutura permite consultas analíticas eficientes, garantindo integridade referencial, performance e flexibilidade para análises de negócio.

---
