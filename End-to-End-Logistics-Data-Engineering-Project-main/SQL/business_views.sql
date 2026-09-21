-- Final Databricks business views
-- Catalog: logistics_cata
-- Schema: gold
-- Reporting layer intentionally stops at Databricks.
-- Azure SQL is not part of the final project scope.

CREATE OR REPLACE VIEW logistics_cata.gold.vw_shipment_status AS
SELECT
    shipment_status,
    COUNT(*) AS shipment_count
FROM logistics_cata.gold.fact_shipments
GROUP BY shipment_status;


CREATE OR REPLACE VIEW logistics_cata.gold.vw_orders_by_priority AS
SELECT
    priority,
    COUNT(*) AS order_count,
    ROUND(SUM(order_value), 2) AS total_order_value
FROM logistics_cata.gold.fact_orders
GROUP BY priority;


CREATE OR REPLACE VIEW logistics_cata.gold.vw_delivery_performance AS
SELECT
    shipment_status,
    COUNT(*) AS total_shipments,
    ROUND(AVG(delivery_duration_hours), 2) AS avg_delivery_hours,
    ROUND(AVG(weight_kg), 2) AS avg_weight_kg
FROM logistics_cata.gold.fact_shipments
GROUP BY shipment_status;
