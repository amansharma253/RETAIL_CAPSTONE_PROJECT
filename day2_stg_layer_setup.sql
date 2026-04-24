-- ============================================================
-- DAY 2: VALIDATION, STAGING & ORDER LINES BRIDGE
-- ============================================================
-- IDMC landed raw data in Day 1. Now we:
--   1. Create the STG schema and transient tables
--   2. Validate & clean RAW data with DQ rules
--   3. Flag invalid records (not discard — all rows land in STG)
--   4. Build the STG_ORDER_ITEMS bridge table
--
-- DQ Rules Applied:
--   CUSTOMERS  : UPPER(STATE, REGION, SEGMENT)
--   ORDERS     : parse MM/DD/YYYY -> DATE, UPPER status/payment,
--                filter: valid date, amount > 0, known status
--   PRODUCTS   : TRIM text fields
--   SHIPMENTS  : parse dates, flag IS_VALID + REJECTION_REASON
--                (UNPARSEABLE_DATE, DELIVERY_BEFORE_SHIPMENT,
--                 SHIPMENT_BEFORE_ORDER)
--   INVENTORY  : cast LAST_UPDATED to DATE, filter nulls/negatives
--   EVENTS     : flatten JSON, parse timestamp, filter known statuses
-- ============================================================


-- ############################################################
-- SECTION 1: CREATE STG SCHEMA + TABLES
-- ############################################################

create schema if not exists CAPSTONE.STG;

create or replace transient table CAPSTONE.STG.STG_CUSTOMERS (
    CUSTOMER_ID      VARCHAR(10),
    CUSTOMER_NAME    VARCHAR(100),
    EMAIL            VARCHAR(150),
    PHONE            VARCHAR(20),
    CITY             VARCHAR(50),
    STATE            VARCHAR(10),
    REGION           VARCHAR(20),
    CUSTOMER_SEGMENT VARCHAR(20)
);

create or replace transient table CAPSTONE.STG.STG_ORDERS (
    ORDER_ID       VARCHAR(10),
    CUSTOMER_ID    VARCHAR(10),
    ORDER_DATE     DATE,
    ORDER_STATUS   VARCHAR(20),
    TOTAL_AMOUNT   NUMBER(10,2),
    PAYMENT_METHOD VARCHAR(20),
    CHANNEL        VARCHAR(10)
);

create or replace transient table CAPSTONE.STG.STG_PRODUCTS (
    PRODUCT_ID   VARCHAR(10),
    PRODUCT_NAME VARCHAR(100),
    CATEGORY     VARCHAR(50),
    SUB_CATEGORY VARCHAR(50),
    PRICE        NUMBER(10,2)
);

create or replace transient table CAPSTONE.STG.STG_SHIPMENTS (
    SHIPMENT_ID      VARCHAR(20),
    ORDER_ID         VARCHAR(20),
    WAREHOUSE_ID     VARCHAR(10),
    SHIPMENT_DATE    DATE,
    DELIVERY_DATE    DATE,
    SHIPMENT_STATUS  VARCHAR(20),
    IS_VALID         BOOLEAN,
    REJECTION_REASON VARCHAR(50)
);

create or replace transient table CAPSTONE.STG.STG_INVENTORY (
    INVENTORY_ID   VARCHAR(10),
    PRODUCT_ID     VARCHAR(10),
    WAREHOUSE_ID   VARCHAR(10),
    STOCK_QUANTITY NUMBER(10,0),
    REORDER_LEVEL  NUMBER(10,0),
    LAST_UPDATED   DATE
);

create or replace transient table CAPSTONE.STG.STG_EVENTS (
    EVENT_ID        VARCHAR(20),
    ORDER_ID        VARCHAR(20),
    EVENT_TYPE      VARCHAR(30),
    EVENT_TIMESTAMP TIMESTAMP_NTZ(9),
    EVENT_STATUS    VARCHAR(20)
);

create or replace transient table CAPSTONE.STG.STG_ORDER_ITEMS (
    ORDER_ITEM_ID VARCHAR(20),
    ORDER_ID      VARCHAR(10),
    PRODUCT_ID    VARCHAR(10),
    QUANTITY      NUMBER(10,0),
    UNIT_PRICE    NUMBER(10,2),
    LINE_TOTAL    NUMBER(10,2)
);


-- ############################################################
-- SECTION 2: RAW -> STG VALIDATED LOADS
-- ############################################################


-- 2A: Customers
-- DQ: UPPER(STATE, REGION, CUSTOMER_SEGMENT)
insert into CAPSTONE.STG.STG_CUSTOMERS
select
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    PHONE,
    CITY,
    upper(trim(STATE))            as STATE,
    upper(trim(REGION))           as REGION,
    upper(trim(CUSTOMER_SEGMENT)) as CUSTOMER_SEGMENT
from CAPSTONE.CAPSTONE.RAW_CUSTOMERS;


-- 2B: Orders
-- DQ: parse date, UPPER status/payment, LOWER channel
-- Filter: valid date, amount > 0, known status
insert into CAPSTONE.STG.STG_ORDERS
select
    ORDER_ID,
    CUSTOMER_ID,
    try_to_date(ORDER_DATE, 'MM/DD/YYYY') as ORDER_DATE,
    upper(trim(ORDER_STATUS))              as ORDER_STATUS,
    TOTAL_AMOUNT,
    upper(trim(PAYMENT_METHOD))            as PAYMENT_METHOD,
    upper(trim(CHANNEL))                   as CHANNEL
from CAPSTONE.CAPSTONE.RAW_ORDERS
where try_to_date(ORDER_DATE, 'MM/DD/YYYY') is not null
  and TOTAL_AMOUNT > 0
  and upper(trim(ORDER_STATUS)) in ('PENDING','PLACED','SHIPPED','DELIVERED','CANCELLED','RETURNED');


-- 2C: Products
-- DQ: TRIM text fields
insert into CAPSTONE.STG.STG_PRODUCTS
select
    PRODUCT_ID,
    trim(PRODUCT_NAME)  as PRODUCT_NAME,
    trim(CATEGORY)      as CATEGORY,
    trim(SUB_CATEGORY)  as SUB_CATEGORY,
    PRICE
from CAPSTONE.CAPSTONE.RAW_PRODUCTS;


-- 2D: Shipments
-- DQ: parse dates, then validate with 3 checks:
--   1. Are dates parseable?
--   2. Is delivery_date >= shipment_date?
--   3. Is shipment_date >= order_date?
-- ALL rows land in STG (valid + invalid), flagged via IS_VALID + REJECTION_REASON
insert into CAPSTONE.STG.STG_SHIPMENTS
with parsed_shipments as (
    select
        SHIPMENT_ID,
        ORDER_ID,
        WAREHOUSE_ID,
        try_to_date(SHIPMENT_DATE, 'MM/DD/YYYY') as SHIPMENT_DATE,
        try_to_date(DELIVERY_DATE, 'MM/DD/YYYY') as DELIVERY_DATE,
        upper(trim(SHIPMENT_STATUS))              as SHIPMENT_STATUS
    from CAPSTONE.CAPSTONE.RAW_SHIPMENTS
)
select
    s.SHIPMENT_ID,
    s.ORDER_ID,
    s.WAREHOUSE_ID,
    s.SHIPMENT_DATE,
    s.DELIVERY_DATE,
    s.SHIPMENT_STATUS,
    case
        when s.SHIPMENT_DATE is null or s.DELIVERY_DATE is null
            then false
        when s.DELIVERY_DATE < s.SHIPMENT_DATE
            then false
        when exists (
            select 1 from CAPSTONE.STG.STG_ORDERS o
            where o.ORDER_ID = s.ORDER_ID
              and s.SHIPMENT_DATE < o.ORDER_DATE
        )
            then false
        else true
    end as IS_VALID,
    case
        when s.SHIPMENT_DATE is null or s.DELIVERY_DATE is null
            then 'UNPARSEABLE_DATE'
        when s.DELIVERY_DATE < s.SHIPMENT_DATE
            then 'DELIVERY_BEFORE_SHIPMENT'
        when exists (
            select 1 from CAPSTONE.STG.STG_ORDERS o
            where o.ORDER_ID = s.ORDER_ID
              and s.SHIPMENT_DATE < o.ORDER_DATE
        )
            then 'SHIPMENT_BEFORE_ORDER'
        else null
    end as REJECTION_REASON
from parsed_shipments s;


-- 2E: Inventory
-- DQ: cast LAST_UPDATED to DATE, filter null products / negative quantities
insert into CAPSTONE.STG.STG_INVENTORY
select
    INVENTORY_ID,
    PRODUCT_ID,
    WAREHOUSE_ID,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    LAST_UPDATED::DATE as LAST_UPDATED
from CAPSTONE.CAPSTONE.RAW_INVENTORY
where PRODUCT_ID is not null
  and STOCK_QUANTITY >= 0
  and REORDER_LEVEL >= 0;


-- 2F: Events (JSON flattening)
-- DQ: extract from VARIANT, parse timestamp, UPPER type/status,
--     filter: non-null keys, known statuses only
insert into CAPSTONE.STG.STG_EVENTS
select
    RAW:event_id::varchar                               as EVENT_ID,
    RAW:order_id::varchar                               as ORDER_ID,
    upper(trim(RAW:event_type::varchar))                as EVENT_TYPE,
    try_to_timestamp_ntz(RAW:event_timestamp::varchar)  as EVENT_TIMESTAMP,
    upper(trim(RAW:event_status::varchar))              as EVENT_STATUS
from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON
where RAW:event_id is not null
  and RAW:order_id is not null
  and try_to_timestamp_ntz(RAW:event_timestamp::varchar) is not null
  and upper(trim(RAW:event_status::varchar)) in ('SUCCESS', 'FAILED');


-- ############################################################
-- SECTION 3: ORDER LINES BRIDGE TABLE (STG_ORDER_ITEMS)
-- ############################################################
-- Each order is split into 1-3 line items.
-- Multi-item orders (2-3 items) have QUANTITY and LINE_TOTAL populated.
-- Single-item orders have NULL QUANTITY and NULL LINE_TOTAL.
-- ORDER_ITEM_ID pattern: <ORDER_ID>-<seq_num>
-- ############################################################

insert into CAPSTONE.STG.STG_ORDER_ITEMS
with order_item_count as (
    select
        ORDER_ID,
        TOTAL_AMOUNT,
        abs(mod(hash(ORDER_ID), 3)) + 1 as NUM_ITEMS
    from CAPSTONE.STG.STG_ORDERS
),
item_sequence as (
    select
        oc.ORDER_ID,
        oc.TOTAL_AMOUNT,
        oc.NUM_ITEMS,
        seq.seq_num
    from order_item_count oc,
    lateral (
        select row_number() over (order by null) as seq_num
        from table(generator(rowcount => 3))
    ) seq
    where seq.seq_num <= oc.NUM_ITEMS
),
assigned_products as (
    select
        i.ORDER_ID,
        i.TOTAL_AMOUNT,
        i.NUM_ITEMS,
        i.seq_num,
        p.PRODUCT_ID,
        p.PRICE as UNIT_PRICE
    from item_sequence i
    join CAPSTONE.STG.STG_PRODUCTS p
        on p.PRODUCT_ID = (
            select PRODUCT_ID
            from CAPSTONE.STG.STG_PRODUCTS
            order by hash(i.ORDER_ID || '-' || i.seq_num || PRODUCT_ID)
            limit 1 offset mod(abs(hash(i.ORDER_ID || '-' || i.seq_num)),
                               (select count(*) from CAPSTONE.STG.STG_PRODUCTS))
        )
)
select
    ORDER_ID || '-' || seq_num   as ORDER_ITEM_ID,
    ORDER_ID,
    PRODUCT_ID,
    case when NUM_ITEMS > 1 then 1 else null end as QUANTITY,
    UNIT_PRICE,
    case when NUM_ITEMS > 1
        then round(TOTAL_AMOUNT * (UNIT_PRICE / sum(UNIT_PRICE) over (partition by ORDER_ID)), 2)
        else null
    end as LINE_TOTAL
from assigned_products;


-- ############################################################
-- SECTION 4: POST-LOAD VALIDATION
-- ############################################################

-- Row count comparison (RAW vs STG)
select 'CUSTOMERS'  as tbl, (select count(*) from CAPSTONE.CAPSTONE.RAW_CUSTOMERS)   as raw_cnt, (select count(*) from CAPSTONE.STG.STG_CUSTOMERS)  as stg_cnt
union all select 'ORDERS',    (select count(*) from CAPSTONE.CAPSTONE.RAW_ORDERS),      (select count(*) from CAPSTONE.STG.STG_ORDERS)
union all select 'PRODUCTS',  (select count(*) from CAPSTONE.CAPSTONE.RAW_PRODUCTS),    (select count(*) from CAPSTONE.STG.STG_PRODUCTS)
union all select 'SHIPMENTS', (select count(*) from CAPSTONE.CAPSTONE.RAW_SHIPMENTS),   (select count(*) from CAPSTONE.STG.STG_SHIPMENTS)
union all select 'INVENTORY', (select count(*) from CAPSTONE.CAPSTONE.RAW_INVENTORY),   (select count(*) from CAPSTONE.STG.STG_INVENTORY)
union all select 'EVENTS',    (select count(*) from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON), (select count(*) from CAPSTONE.STG.STG_EVENTS)
order by tbl;

-- Shipment DQ summary
select IS_VALID, REJECTION_REASON, count(*) as cnt
from CAPSTONE.STG.STG_SHIPMENTS
group by IS_VALID, REJECTION_REASON
order by IS_VALID desc, cnt desc;

-- Order Items bridge summary
select
    case when QUANTITY is null then 'SINGLE_ITEM (null qty)' else 'MULTI_ITEM' end as item_type,
    count(*) as line_count,
    count(distinct ORDER_ID) as order_count
from CAPSTONE.STG.STG_ORDER_ITEMS
group by item_type;
