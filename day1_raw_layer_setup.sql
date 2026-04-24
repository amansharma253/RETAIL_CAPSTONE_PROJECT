-- ============================================================
-- DAY 1: POST-IDMC LANDING VALIDATION
-- ============================================================
-- The database, schemas, and all RAW/REJECT tables were created
-- at runtime by Informatica IDMC during ingestion. IDMC handled:
--   - Table creation (RAW_*, REJECT_*, INVALID_SHIPMENTS)
--   - Data extraction from source systems
--   - Type mapping and initial reject routing
--
-- This script validates that data landed successfully.
-- ============================================================


-- 1. Verify all tables exist
show tables in schema CAPSTONE.CAPSTONE;


-- 2. Row counts for each RAW table (confirm data arrived)
select 'RAW_CUSTOMERS'   as table_name, count(*) as row_count from CAPSTONE.CAPSTONE.RAW_CUSTOMERS
union all
select 'RAW_ORDERS',                    count(*)              from CAPSTONE.CAPSTONE.RAW_ORDERS
union all
select 'RAW_PRODUCTS',                  count(*)              from CAPSTONE.CAPSTONE.RAW_PRODUCTS
union all
select 'RAW_SHIPMENTS',                 count(*)              from CAPSTONE.CAPSTONE.RAW_SHIPMENTS
union all
select 'RAW_INVENTORY',                 count(*)              from CAPSTONE.CAPSTONE.RAW_INVENTORY
union all
select 'RAW_EVENTS_JSON',               count(*)              from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON
order by table_name;


-- 3. Row counts for REJECT tables (should be 0 or minimal)
select 'REJECT_CUSTOMERS'  as table_name, count(*) as row_count from CAPSTONE.CAPSTONE.REJECT_CUSTOMERS
union all
select 'REJECT_ORDERS',                   count(*)              from CAPSTONE.CAPSTONE.REJECT_ORDERS
union all
select 'REJECT_PRODUCTS',                 count(*)              from CAPSTONE.CAPSTONE.REJECT_PRODUCTS
union all
select 'REJECT_INVENTORY',                count(*)              from CAPSTONE.CAPSTONE.REJECT_INVENTORY
union all
select 'INVALID_SHIPMENTS',               count(*)              from CAPSTONE.CAPSTONE.INVALID_SHIPMENTS
order by table_name;


-- 4. Sample data checks (first 5 rows from each RAW table)
select * from CAPSTONE.CAPSTONE.RAW_CUSTOMERS   limit 5;
select * from CAPSTONE.CAPSTONE.RAW_ORDERS      limit 5;
select * from CAPSTONE.CAPSTONE.RAW_PRODUCTS    limit 5;
select * from CAPSTONE.CAPSTONE.RAW_SHIPMENTS   limit 5;
select * from CAPSTONE.CAPSTONE.RAW_INVENTORY   limit 5;
select * from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON limit 5;


-- 5. Null/empty checks on primary key columns
select 'RAW_CUSTOMERS'  as table_name, count(*) as null_pk_count from CAPSTONE.CAPSTONE.RAW_CUSTOMERS   where CUSTOMER_ID  is null
union all
select 'RAW_ORDERS',                    count(*)                  from CAPSTONE.CAPSTONE.RAW_ORDERS      where ORDER_ID     is null
union all
select 'RAW_PRODUCTS',                  count(*)                  from CAPSTONE.CAPSTONE.RAW_PRODUCTS    where PRODUCT_ID   is null
union all
select 'RAW_SHIPMENTS',                 count(*)                  from CAPSTONE.CAPSTONE.RAW_SHIPMENTS   where SHIPMENT_ID  is null
union all
select 'RAW_INVENTORY',                 count(*)                  from CAPSTONE.CAPSTONE.RAW_INVENTORY   where INVENTORY_ID is null
union all
select 'RAW_EVENTS_JSON',               count(*)                  from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON where RAW:event_id is null
order by table_name;


-- 6. Duplicate check on primary keys
select 'RAW_CUSTOMERS'  as table_name, count(*) as duplicate_count from CAPSTONE.CAPSTONE.RAW_CUSTOMERS   group by CUSTOMER_ID  having count(*) > 1
union all
select 'RAW_ORDERS',                    count(*)                    from CAPSTONE.CAPSTONE.RAW_ORDERS      group by ORDER_ID     having count(*) > 1
union all
select 'RAW_PRODUCTS',                  count(*)                    from CAPSTONE.CAPSTONE.RAW_PRODUCTS    group by PRODUCT_ID   having count(*) > 1
union all
select 'RAW_SHIPMENTS',                 count(*)                    from CAPSTONE.CAPSTONE.RAW_SHIPMENTS   group by SHIPMENT_ID  having count(*) > 1
union all
select 'RAW_INVENTORY',                 count(*)                    from CAPSTONE.CAPSTONE.RAW_INVENTORY   group by INVENTORY_ID having count(*) > 1;


-- 7. Referential integrity spot-checks
-- Orders referencing non-existent customers
select count(*) as orphan_orders
from CAPSTONE.CAPSTONE.RAW_ORDERS o
where not exists (
    select 1 from CAPSTONE.CAPSTONE.RAW_CUSTOMERS c
    where c.CUSTOMER_ID = o.CUSTOMER_ID
);

-- Shipments referencing non-existent orders
select count(*) as orphan_shipments
from CAPSTONE.CAPSTONE.RAW_SHIPMENTS s
where not exists (
    select 1 from CAPSTONE.CAPSTONE.RAW_ORDERS o
    where o.ORDER_ID = s.ORDER_ID
);


-- 8. INVALID_SHIPMENTS audit (rows IDMC flagged as delivery < shipment)
select count(*) as invalid_count from CAPSTONE.CAPSTONE.INVALID_SHIPMENTS;
select * from CAPSTONE.CAPSTONE.INVALID_SHIPMENTS limit 5;


-- 9. Enable change tracking (needed for Day 4 streams)
alter table CAPSTONE.CAPSTONE.RAW_ORDERS      set change_tracking = true;
alter table CAPSTONE.CAPSTONE.RAW_SHIPMENTS   set change_tracking = true;
alter table CAPSTONE.CAPSTONE.RAW_INVENTORY   set change_tracking = true;
alter table CAPSTONE.CAPSTONE.RAW_EVENTS_JSON set change_tracking = true;
