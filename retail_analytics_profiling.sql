-- RETAIL ORDER, INVENTORY & FULFILLMENT ANALYTICS PLATFORM
-- COMPREHENSIVE DATA PROFILING AND ANOMALY DETECTION
-- Scope: ORA_CUSTOMERS, ORA_ORDERS, ORA_PRODUCTS, INVENTORY, SHIPMENTS, ORDER_EVENTS

-- ============================================================
-- SECTION 1: SOURCE TABLE OVERVIEW
-- ============================================================

-- 1.1 Row counts across all source tables
SELECT 'ORA_CUSTOMERS' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS
UNION ALL
SELECT 'ORA_ORDERS' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
UNION ALL
SELECT 'ORA_PRODUCTS' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS
UNION ALL
SELECT 'INVENTORY' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.INVENTORY
UNION ALL
SELECT 'SHIPMENTS' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS
UNION ALL
SELECT 'ORDER_EVENTS' AS table_name, COUNT(*) AS row_count FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS
ORDER BY table_name;

-- ============================================================
-- SECTION 2: ORA_CUSTOMERS PROFILING
-- ============================================================

-- 2.1 Null and distinct profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CUSTOMER_ID) AS distinct_customer_id,
  COUNT(DISTINCT EMAIL) AS distinct_email,
  SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN CUSTOMER_NAME IS NULL THEN 1 ELSE 0 END) AS null_customer_name,
  SUM(CASE WHEN EMAIL IS NULL THEN 1 ELSE 0 END) AS null_email,
  SUM(CASE WHEN PHONE IS NULL THEN 1 ELSE 0 END) AS null_phone,
  SUM(CASE WHEN CITY IS NULL THEN 1 ELSE 0 END) AS null_city,
  SUM(CASE WHEN STATE IS NULL THEN 1 ELSE 0 END) AS null_state,
  SUM(CASE WHEN REGION IS NULL THEN 1 ELSE 0 END) AS null_region,
  SUM(CASE WHEN CUSTOMER_SEGMENT IS NULL THEN 1 ELSE 0 END) AS null_customer_segment
FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS;

-- 2.2 Duplicate customer IDs
SELECT CUSTOMER_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS
GROUP BY CUSTOMER_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, CUSTOMER_ID;

-- 2.3 Duplicate emails
SELECT EMAIL, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS
WHERE EMAIL IS NOT NULL
GROUP BY EMAIL
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, EMAIL;

-- 2.4 Invalid email formats
SELECT *
FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS
WHERE EMAIL IS NULL
   OR EMAIL NOT LIKE '%@%.%';

-- 2.5 Region and segment distribution
SELECT REGION, CUSTOMER_SEGMENT, COUNT(*) AS customer_count
FROM CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS
GROUP BY REGION, CUSTOMER_SEGMENT
ORDER BY REGION, customer_count DESC;

-- ============================================================
-- SECTION 3: ORA_PRODUCTS PROFILING
-- ============================================================
-- 3.1 Null, distinct and price profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT PRODUCT_ID) AS distinct_product_id,
  COUNT(DISTINCT PRODUCT_NAME) AS distinct_product_name,
  SUM(CASE WHEN PRODUCT_ID IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN PRODUCT_NAME IS NULL THEN 1 ELSE 0 END) AS null_product_name,
  SUM(CASE WHEN CATEGORY IS NULL THEN 1 ELSE 0 END) AS null_category,
  SUM(CASE WHEN SUB_CATEGORY IS NULL THEN 1 ELSE 0 END) AS null_sub_category,
  SUM(CASE WHEN PRICE IS NULL THEN 1 ELSE 0 END) AS null_price,
  MIN(PRICE) AS min_price,
  MAX(PRICE) AS max_price,
  AVG(PRICE) AS avg_price
FROM CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS;

-- 3.2 Duplicate product IDs
SELECT PRODUCT_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS
GROUP BY PRODUCT_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, PRODUCT_ID;

-- 3.3 Invalid or suspicious prices
SELECT *
FROM CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS
WHERE PRICE IS NULL
   OR PRICE <= 0;

-- 3.4 Product distribution by category and sub-category
SELECT CATEGORY, SUB_CATEGORY, COUNT(*) AS product_cou
nt
FROM CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS
GROUP BY CATEGORY, SUB_CATEGORY
ORDER BY CATEGORY, product_count DESC;

-- ============================================================
-- SECTION 4: ORA_ORDERS PROFILING
-- ============================================================
-- 4.1 Nulls, distincts and monetary profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT ORDER_ID) AS distinct_order_id,
  COUNT(DISTINCT CUSTOMER_ID) AS distinct_customer_id,
  SUM(CASE WHEN ORDER_ID IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN CUSTOMER_ID IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
  SUM(CASE WHEN ORDER_DATE IS NULL THEN 1 ELSE 0 END) AS null_order_date,
  SUM(CASE WHEN ORDER_STATUS IS NULL THEN 1 ELSE 0 END) AS null_order_status,
  SUM(CASE WHEN TOTAL_AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_total_amount,
  SUM(CASE WHEN PAYMENT_METHOD IS NULL THEN 1 ELSE 0 END) AS null_payment_method,
  SUM(CASE WHEN CHANNEL IS NULL THEN 1 ELSE 0 END) AS null_channel,
  MIN(TOTAL_AMOUNT) AS min_total_amount,
  MAX(TOTAL_AMOUNT) AS max_total_amount,
  AVG(TOTAL_AMOUNT) AS avg_total_amount
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS;

-- 4.2 Duplicate orders
SELECT ORDER_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
GROUP BY ORDER_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, ORDER_ID;

-- 4.3 Invalid order statuses for business process
SELECT ORDER_STATUS, COUNT(*) AS row_count
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
GROUP BY ORDER_STATUS
ORDER BY row_count DESC;

-- 4.4 Orders with invalid status values
SELECT *
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
WHERE UPPER(TRIM(ORDER_STATUS)) NOT IN ('PLACED', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED', 'PENDING', 'PROCESSING');

-- 4.5 Orders with non-positive amounts
SELECT *
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
WHERE TOTAL_AMOUNT IS NULL
   OR TOTAL_AMOUNT <= 0;

-- 4.6 Orders by month and channel
SELECT
  DATE_TRUNC('MONTH', ORDER_DATE) AS order_month,
  CHANNEL,
  COUNT(*) AS order_count,
  SUM(TOTAL_AMOUNT) AS total_sales
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
GROUP BY order_month, CHANNEL
ORDER BY order_month, CHANNEL;

-- 4.7 Orders with missing customer master links
SELECT o.*
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORA_CUSTOMERS c
  ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE c.CUSTOMER_ID IS NULL;

-- ============================================================
-- SECTION 5: INVENTORY PROFILING
-- ============================================================
-- 5.1 Nulls, distincts and stock profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT INVENTORY_ID) AS distinct_inventory_id,
  COUNT(DISTINCT PRODUCT_ID) AS distinct_product_id,
  COUNT(DISTINCT WAREHOUSE_ID) AS distinct_warehouse_id,
  SUM(CASE WHEN INVENTORY_ID IS NULL THEN 1 ELSE 0 END) AS null_inventory_id,
  SUM(CASE WHEN PRODUCT_ID IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN WAREHOUSE_ID IS NULL THEN 1 ELSE 0 END) AS null_warehouse_id,
  SUM(CASE WHEN STOCK_QUANTITY IS NULL THEN 1 ELSE 0 END) AS null_stock_quantity,
  SUM(CASE WHEN REORDER_LEVEL IS NULL THEN 1 ELSE 0 END) AS null_reorder_level,
  MIN(STOCK_QUANTITY) AS min_stock_quantity,
  MAX(STOCK_QUANTITY) AS max_stock_quantity,
  AVG(STOCK_QUANTITY) AS avg_stock_quantity
FROM CAPSTONE_EDA.PUBLIC.INVENTORY;

-- 5.2 Duplicate inventory IDs
SELECT INVENTORY_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.INVENTORY
GROUP BY INVENTORY_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, INVENTORY_ID;

-- 5.3 Inventory rows missing product links
SELECT i.*
FROM CAPSTONE_EDA.PUBLIC.INVENTORY i
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS p
  ON i.PRODUCT_ID = p.PRODUCT_ID
WHERE i.PRODUCT_ID IS NULL
   OR p.PRODUCT_ID IS NULL;

-- 5.4 Negative stock anomaly
SELECT *
FROM CAPSTONE_EDA.PUBLIC.INVENTORY
WHERE STOCK_QUANTITY < 0;

-- 5.5 Low-stock and stockout anomaly
SELECT *
FROM CAPSTONE_EDA.PUBLIC.INVENTORY
WHERE STOCK_QUANTITY <= REORDER_LEVEL
ORDER BY STOCK_QUANTITY, REORDER_LEVEL;

-- 5.6 Warehouse stock position summary
SELECT
  WAREHOUSE_ID,
  COUNT(*) AS sku_count,
  SUM(STOCK_QUANTITY) AS total_stock,
  SUM(CASE WHEN STOCK_QUANTITY <= REORDER_LEVEL THEN 1 ELSE 0 END) AS low_stock_sku_count
FROM CAPSTONE_EDA.PUBLIC.INVENTORY
GROUP BY WAREHOUSE_ID
ORDER BY total_stock DESC;

-- ============================================================
-- SECTION 6: SHIPMENTS PROFILING
-- ============================================================

-- 6.1 Nulls, distincts and date profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT SHIPMENT_ID) AS distinct_shipment_id,
  COUNT(DISTINCT ORDER_ID) AS distinct_order_id,
  COUNT(DISTINCT WAREHOUSE_ID) AS distinct_warehouse_id,
  SUM(CASE WHEN SHIPMENT_ID IS NULL THEN 1 ELSE 0 END) AS null_shipment_id,
  SUM(CASE WHEN ORDER_ID IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN WAREHOUSE_ID IS NULL THEN 1 ELSE 0 END) AS null_warehouse_id,
  SUM(CASE WHEN SHIPMENT_DATE IS NULL THEN 1 ELSE 0 END) AS null_shipment_date,
  SUM(CASE WHEN DELIVERY_DATE IS NULL THEN 1 ELSE 0 END) AS null_delivery_date,
  SUM(CASE WHEN SHIPMENT_STATUS IS NULL THEN 1 ELSE 0 END) AS null_shipment_status,
  MIN(SHIPMENT_DATE) AS min_shipment_date,
  MAX(SHIPMENT_DATE) AS max_shipment_date
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS;

-- 6.2 Duplicate shipment IDs
SELECT SHIPMENT_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS
GROUP BY SHIPMENT_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, SHIPMENT_ID;

-- 6.3 Shipment status values
SELECT SHIPMENT_STATUS, COUNT(*) AS row_count
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS
GROUP BY SHIPMENT_STATUS
ORDER BY row_count DESC;

-- 6.4 Invalid shipment chronology: delivery before shipment
SELECT *
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS
WHERE DELIVERY_DATE < SHIPMENT_DATE;

-- 6.5 Invalid fulfillment chronology: shipment before order date
SELECT s.*, o.ORDER_DATE
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS s
JOIN CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
  ON s.ORDER_ID = o.ORDER_ID
WHERE s.SHIPMENT_DATE < o.ORDER_DATE;

-- 6.6 Shipment rows without matching orders
SELECT s.*
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS s
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
  ON s.ORDER_ID = o.ORDER_ID
WHERE o.ORDER_ID IS NULL;

-- 6.7 Delivery lead time summary
SELECT
  MIN(DATEDIFF('DAY', SHIPMENT_DATE, DELIVERY_DATE)) AS min_delivery_days,
  MAX(DATEDIFF('DAY', SHIPMENT_DATE, DELIVERY_DATE)) AS max_delivery_days,
  AVG(DATEDIFF('DAY', SHIPMENT_DATE, DELIVERY_DATE)) AS avg_delivery_days
FROM CAPSTONE_EDA.PUBLIC.SHIPMENTS
WHERE DELIVERY_DATE IS NOT NULL;

-- ============================================================
-- SECTION 7: ORDER_EVENTS PROFILING
-- ============================================================
-- 7.1 Nulls, distincts and timestamp profile
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT EVENT_ID) AS distinct_event_id,
  COUNT(DISTINCT ORDER_ID) AS distinct_order_id,
  SUM(CASE WHEN EVENT_ID IS NULL THEN 1 ELSE 0 END) AS null_event_id,
  SUM(CASE WHEN ORDER_ID IS NULL THEN 1 ELSE 0 END) AS null_order_id,
  SUM(CASE WHEN EVENT_TYPE IS NULL THEN 1 ELSE 0 END) AS null_event_type,
  SUM(CASE WHEN EVENT_STATUS IS NULL THEN 1 ELSE 0 END) AS null_event_status,
  SUM(CASE WHEN EVENT_TIMESTAMP IS NULL THEN 1 ELSE 0 END) AS null_event_timestamp,
  MIN(EVENT_TIMESTAMP) AS min_event_timestamp,
  MAX(EVENT_TIMESTAMP) AS max_event_timestamp
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS;

-- 7.2 Duplicate event IDs
SELECT EVENT_ID, COUNT(*) AS duplicate_count
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS
GROUP BY EVENT_ID
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC, EVENT_ID;

-- 7.3 Event type distribution
SELECT EVENT_TYPE, EVENT_STATUS, COUNT(*) AS row_count
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS
GROUP BY EVENT_TYPE, EVENT_STATUS
ORDER BY EVENT_TYPE, row_count DESC;

-- 7.4 Events without matching orders
SELECT e.*
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS e
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
  ON e.ORDER_ID = o.ORDER_ID
WHERE o.ORDER_ID IS NULL;

-- 7.5 Event timestamps earlier than order date
SELECT e.*, o.ORDER_DATE
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS e
JOIN CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
  ON e.ORDER_ID = o.ORDER_ID
WHERE CAST(e.EVENT_TIMESTAMP AS DATE) < o.ORDER_DATE;

-- 7.6 Potential malformed events based on key nulls
SELECT *
FROM CAPSTONE_EDA.PUBLIC.ORDER_EVENTS
WHERE EVENT_ID IS NULL
   OR ORDER_ID IS NULL
   OR EVENT_TYPE IS NULL
   OR EVENT_TIMESTAMP IS NULL;

-- ============================================================
-- SECTION 8: CROSS-TABLE BUSINESS ANOMALIES
-- ============================================================

-- 8.1 Duplicate orders impact summary
SELECT COUNT(*) AS duplicate_order_groups
FROM (
  SELECT ORDER_ID
  FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS
  GROUP BY ORDER_ID
  HAVING COUNT(*) > 1
) d;

-- 8.2 Orders with no shipments
SELECT o.ORDER_ID, o.CUSTOMER_ID, o.ORDER_DATE, o.ORDER_STATUS
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
LEFT JOIN CAPSTONE_EDA.PUBLIC.SHIPMENTS s
  ON o.ORDER_ID = s.ORDER_ID
WHERE s.ORDER_ID IS NULL;

-- 8.3 Delivered orders without delivered shipment records
SELECT o.ORDER_ID, o.ORDER_STATUS, s.SHIPMENT_STATUS
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
LEFT JOIN CAPSTONE_EDA.PUBLIC.SHIPMENTS s
  ON o.ORDER_ID = s.ORDER_ID
WHERE UPPER(TRIM(o.ORDER_STATUS)) = 'DELIVERED'
  AND (s.SHIPMENT_STATUS IS NULL OR UPPER(TRIM(s.SHIPMENT_STATUS)) <> 'DELIVERED');

-- 8.4 Cancelled orders that still shipped
SELECT o.ORDER_ID, o.ORDER_STATUS, s.SHIPMENT_STATUS, s.SHIPMENT_DATE
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
JOIN CAPSTONE_EDA.PUBLIC.SHIPMENTS s
  ON o.ORDER_ID = s.ORDER_ID
WHERE UPPER(TRIM(o.ORDER_STATUS)) = 'CANCELLED';

-- 8.5 Orders missing event trail
SELECT o.ORDER_ID, o.ORDER_STATUS, o.ORDER_DATE
FROM CAPSTONE_EDA.PUBLIC.ORA_ORDERS o
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORDER_EVENTS e
  ON o.ORDER_ID = e.ORDER_ID
WHERE e.ORDER_ID IS NULL;

-- 8.6 Inventory-product coverage summary
SELECT
  COUNT(*) AS inventory_rows,
  COUNT(DISTINCT i.PRODUCT_ID) AS inventory_distinct_products,
  COUNT(DISTINCT p.PRODUCT_ID) AS matched_product_masters
FROM CAPSTONE_EDA.PUBLIC.INVENTORY i
LEFT JOIN CAPSTONE_EDA.PUBLIC.ORA_PRODUCTS p
  ON i.PRODUCT_ID = p.PRODUCT_ID;

-- ============================================================
-- SECTION 9: FINDINGS SUMMARY
-- Update these comments after executing the queries.
-- ============================================================

-- FINDINGS SUMMARY
-- 1. Row-count validation across all 6 source tables completed successfully.
--    ORA_CUSTOMERS=1200, ORA_ORDERS=1800, ORA_PRODUCTS=600, INVENTORY=1200, SHIPMENTS=1800, ORDER_EVENTS=800.
-- 2. Customer master quality is strong.
--    Duplicate CUSTOMER_ID groups=0, duplicate EMAIL groups=0, invalid EMAIL rows=0.
-- 3. Product master quality is strong.
--    Duplicate PRODUCT_ID groups=0 and invalid/non-positive PRICE rows=0.
-- 4. Order management quality is mostly strong at the master level.
--    Duplicate ORDER_ID groups=0, invalid ORDER_STATUS rows=0, invalid/non-positive TOTAL_AMOUNT rows=0, orphan orders to customers=0.
-- 5. Inventory master linkage is strong, but stock-risk exists.
--    Missing PRODUCT_ID rows=0, inventory rows without product master link=0, negative stock rows=0, low-stock rows=115.
-- 6. Fulfillment data contains major chronology anomalies.
--    DELIVERY_DATE earlier than SHIPMENT_DATE rows=871.
--    SHIPMENT_DATE earlier than ORDER_DATE rows=863.
--    Orphan shipment rows to orders=0.
-- 7. Event-stream structure is clean, but business coverage is weak.
--    Malformed event rows=0, duplicate EVENT_ID groups=0, but orders missing any event trail=1154.
-- 8. Key business anomaly conclusion.
--    The most material operational issues are shipment-date inconsistencies and sparse order-event coverage.
--    Inventory also shows 115 low-stock records requiring replenishment attention.
--    Referential integrity across customer, order, product, inventory, and shipment masters is otherwise intact.
