erDiagram
    FACT_SALES ||--o{ DIM_CUSTOMERS : "customer_key"
    FACT_SALES ||--o{ DIM_PRODUCTS : "product_key"
    
    DIM_CUSTOMERS {
        int customer_key PK "Surrogate Key"
        string customer_id "Natural Key"
        string customer_name
        string customer_type
        string email
        string phone
        string address
        string city
        string state
        string country
        date registration_date
        string source_system "CRM ou ERP"
        timestamp effective_date
        timestamp end_date
        boolean is_current
    }
    
    DIM_PRODUCTS {
        int product_key PK "Surrogate Key"
        string product_id "Natural Key"
        string product_name
        string category
        string subcategory
        string brand
        decimal unit_price
        string unit_measure
        boolean is_active "Apenas ativos"
        date start_date "prd_start_dt"
        date end_date "prd_end_dt"
        timestamp effective_date
        timestamp end_date_scd
        boolean is_current
    }
    
    FACT_SALES {
        int sales_key PK "Surrogate Key"
        int customer_key FK "FK → dim_customers"
        int product_key FK "FK → dim_products"
        string order_id "Número do pedido"
        int order_item_seq "Item do pedido"
        date order_date
        date delivery_date
        int quantity
        decimal unit_price
        decimal discount_pct
        decimal tax_amount
        decimal total_amount
        string payment_method
        string order_status
        string location_code
        timestamp created_at
        timestamp updated_at
    }
