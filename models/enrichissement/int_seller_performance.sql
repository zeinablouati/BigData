WITH seller_orders AS (
    -- Joindre les tables sellers et orders
    SELECT
        s.seller_id,
        s.seller_city,
        s.seller_state,
        o.order_id,
        o.order_status,
        o.order_purchase_timestamp,
        o.order_value -- Ajoutez cette colonne si nécessaire
   from {{ source('test', 'products') }} s
    LEFT JOIN {{ source('test', 'orders') }} o
    ON s.seller_id = o.seller_id
),

filtered_orders AS (
    -- Filtrer uniquement les commandes valides (par exemple, statut "delivered")
    SELECT
        seller_id,
        order_id,
        seller_city,
        seller_state,
        order_value,
        order_purchase_timestamp
    FROM seller_orders
    WHERE order_status = 'delivered'
),

seller_metrics AS (
    -- Calculer les métriques par vendeur
    SELECT
        seller_id,
        MIN(seller_city) AS seller_city, -- Pour éviter les doublons
        MIN(seller_state) AS seller_state,
        COUNT(order_id) AS total_orders, -- Nombre total de commandes
        COALESCE(SUM(order_value), 0) AS total_revenue, -- CA total
        MIN(order_purchase_timestamp) AS first_order_date, -- Première commande
        MAX(order_purchase_timestamp) AS last_order_date -- Dernière commande
    FROM filtered_orders
    GROUP BY seller_id
)

SELECT * FROM seller_metrics;
