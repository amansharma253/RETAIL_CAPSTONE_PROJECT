-- ============================================================
-- DAY 3: CURATED STAR SCHEMA
-- ============================================================
-- Builds the star schema to solve core business problems:
--
--   Business Problem              -> Star Schema Solution
--   ─────────────────────────────────────────────────────
--   Unified order lifecycle       -> FACT_ORDER (grain: 1 row per order line item)
--   Real-time inventory           -> DIM_PRODUCT (enriched with stock flags)
--   Warehouse performance         -> DIM_WAREHOUSE (derived dimension)
--   Stockout/overstock detection  -> V_INVENTORY_RECONCILIATION view
--   Customer segmentation         -> DIM_CUSTOMER
--   Temporal analysis             -> DIM_DATE (Q1 2025 calendar)
--
-- Sources: STG tables from Day 2
-- ============================================================

create schema if not exists CAPSTONE.CURATED;


-- ############################################################
-- SECTION 1: DIMENSION TABLES
-- ############################################################

-- DIM_CUSTOMER: Type 1 SCD (overwrite on change)
-- Solves: Customer segmentation, regional analysis
create or replace table CAPSTONE.CURATED.DIM_CUSTOMER (
    DIM_CUSTOMER_KEY NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
    CUSTOMER_ID      VARCHAR(10),
    CUSTOMER_NAME    VARCHAR(100),
    EMAIL            VARCHAR(150),
    PHONE            VARCHAR(20),
    CITY             VARCHAR(50),
    STATE            VARCHAR(10),
    REGION           VARCHAR(20),
    CUSTOMER_SEGMENT VARCHAR(20),
    primary key (DIM_CUSTOMER_KEY)
);

-- DIM_DATE: Calendar dimension (Q1 2025: Jan 1 - Mar 31, 90 days)
-- Solves: Time-based trending, weekend vs weekday analysis
create or replace table CAPSTONE.CURATED.DIM_DATE (
    DATE_KEY    NUMBER(38,0) NOT NULL,
    FULL_DATE   DATE,
    DAY         NUMBER(2,0),
    MONTH       NUMBER(2,0),
    MONTH_NAME  VARCHAR(10),
    QUARTER     NUMBER(1,0),
    YEAR        NUMBER(4,0),
    DAY_OF_WEEK VARCHAR(10),
    IS_WEEKEND  BOOLEAN,
    primary key (DATE_KEY)
);

-- DIM_PRODUCT: Enriched with inventory aggregates
-- Solves: Stockout/overstock identification, product performance
-- IS_LOW_STOCK  = total stock across warehouses <= total reorder level
-- IS_OVERSTOCK  = total stock across warehouses > 3x total reorder level
create or replace table CAPSTONE.CURATED.DIM_PRODUCT (
    DIM_PRODUCT_KEY      NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
    PRODUCT_ID           VARCHAR(10),
    PRODUCT_NAME         VARCHAR(100),
    CATEGORY             VARCHAR(50),
    SUB_CATEGORY         VARCHAR(50),
    PRICE                NUMBER(10,2),
    TOTAL_STOCK_QUANTITY NUMBER(38,0),
    AVG_REORDER_LEVEL    NUMBER(38,0),
    WAREHOUSE_COUNT      NUMBER(38,0),
    IS_LOW_STOCK         BOOLEAN,
    IS_OVERSTOCK         BOOLEAN,
    primary key (DIM_PRODUCT_KEY)
);

-- DIM_WAREHOUSE: Derived dimension (no raw source file)
-- Solves: Warehouse-level performance, regional fulfillment analysis
create or replace table CAPSTONE.CURATED.DIM_WAREHOUSE (
    DIM_WAREHOUSE_KEY NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
    WAREHOUSE_ID      VARCHAR(10),
    WAREHOUSE_NAME    VARCHAR(50),
    REGION            VARCHAR(30),
    primary key (DIM_WAREHOUSE_KEY)
);


-- ############################################################
-- SECTION 2: FACT TABLE
-- ############################################################

-- FACT_ORDER: Grain = one row per order line item
-- Solves: Order lifecycle tracking, fulfillment efficiency,
--         shipment DQ visibility, event correlation
create or replace table CAPSTONE.CURATED.FACT_ORDER (
    FACT_ORDER_KEY      NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 noorder,
    ORDER_ITEM_ID       VARCHAR(20),
    ORDER_ID            VARCHAR(10),
    PRODUCT_KEY         NUMBER(38,0),
    CUSTOMER_KEY        NUMBER(38,0),
    ORDER_DATE_KEY      NUMBER(38,0),
    SHIPMENT_DATE_KEY   NUMBER(38,0),
    DELIVERY_DATE_KEY   NUMBER(38,0),
    WAREHOUSE_KEY       NUMBER(38,0),
    ORDER_STATUS        VARCHAR(20),
    PAYMENT_METHOD      VARCHAR(20),
    CHANNEL             VARCHAR(10),
    SHIPMENT_STATUS     VARCHAR(20),
    QUANTITY            NUMBER(10,0),
    UNIT_PRICE          NUMBER(10,2),
    LINE_TOTAL          NUMBER(10,2),
    ORDER_TOTAL         NUMBER(10,2),
    FULFILLMENT_DAYS    NUMBER(38,0),
    IS_FULFILLED        BOOLEAN,
    IS_VALID_SHIPMENT   BOOLEAN,
    SHIPMENT_DQ_REASON  VARCHAR(50),
    LATEST_EVENT_TYPE   VARCHAR(30),
    LATEST_EVENT_STATUS VARCHAR(20),
    primary key (FACT_ORDER_KEY),
    constraint FK_PRODUCT    foreign key (PRODUCT_KEY)    references CAPSTONE.CURATED.DIM_PRODUCT(DIM_PRODUCT_KEY),
    constraint FK_CUSTOMER   foreign key (CUSTOMER_KEY)   references CAPSTONE.CURATED.DIM_CUSTOMER(DIM_CUSTOMER_KEY),
    constraint FK_ORDER_DATE foreign key (ORDER_DATE_KEY) references CAPSTONE.CURATED.DIM_DATE(DATE_KEY),
    constraint FK_WAREHOUSE  foreign key (WAREHOUSE_KEY)  references CAPSTONE.CURATED.DIM_WAREHOUSE(DIM_WAREHOUSE_KEY)
);


-- ############################################################
-- SECTION 3: LOAD DIMENSIONS
-- ############################################################

-- 3A: DIM_CUSTOMER (direct load from STG)
insert into CAPSTONE.CURATED.DIM_CUSTOMER
    (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY, STATE, REGION, CUSTOMER_SEGMENT)
select
    CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY, STATE, REGION, CUSTOMER_SEGMENT
from CAPSTONE.STG.STG_CUSTOMERS;

-- 3B: DIM_DATE (generated — Q1 2025: 90 days)
insert into CAPSTONE.CURATED.DIM_DATE
select
    to_number(to_char(d.dt, 'YYYYMMDD'))  as DATE_KEY,
    d.dt                                   as FULL_DATE,
    day(d.dt)                              as DAY,
    month(d.dt)                            as MONTH,
    monthname(d.dt)                        as MONTH_NAME,
    quarter(d.dt)                          as QUARTER,
    year(d.dt)                             as YEAR,
    dayname(d.dt)                          as DAY_OF_WEEK,
    dayofweek(d.dt) in (0, 6)             as IS_WEEKEND
from (
    select dateadd(day, seq4(), '2025-01-01'::date) as dt
    from table(generator(rowcount => 90))
) d;

-- 3C: DIM_PRODUCT (enriched with inventory aggregates)
insert into CAPSTONE.CURATED.DIM_PRODUCT
    (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, PRICE,
     TOTAL_STOCK_QUANTITY, AVG_REORDER_LEVEL, WAREHOUSE_COUNT,
     IS_LOW_STOCK, IS_OVERSTOCK)
with inventory_summary as (
    select
        PRODUCT_ID,
        sum(STOCK_QUANTITY)           as TOTAL_STOCK_QUANTITY,
        round(avg(REORDER_LEVEL), 0) as AVG_REORDER_LEVEL,
        count(distinct WAREHOUSE_ID) as WAREHOUSE_COUNT,
        sum(REORDER_LEVEL)           as TOTAL_REORDER_LEVEL
    from CAPSTONE.STG.STG_INVENTORY
    group by PRODUCT_ID
)
select
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUB_CATEGORY,
    p.PRICE,
    coalesce(i.TOTAL_STOCK_QUANTITY, 0),
    coalesce(i.AVG_REORDER_LEVEL, 0),
    coalesce(i.WAREHOUSE_COUNT, 0),
    i.TOTAL_STOCK_QUANTITY is not null
        and i.TOTAL_STOCK_QUANTITY <= i.TOTAL_REORDER_LEVEL,
    i.TOTAL_STOCK_QUANTITY is not null
        and i.TOTAL_STOCK_QUANTITY > 3 * i.TOTAL_REORDER_LEVEL
from CAPSTONE.STG.STG_PRODUCTS p
left join inventory_summary i on p.PRODUCT_ID = i.PRODUCT_ID;

-- 3D: DIM_WAREHOUSE (derived from shipments + inventory, hardcoded mapping)
insert into CAPSTONE.CURATED.DIM_WAREHOUSE
    (WAREHOUSE_ID, WAREHOUSE_NAME, REGION)
with all_warehouses as (
    select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_SHIPMENTS
    union
    select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_INVENTORY
)
select
    WAREHOUSE_ID,
    case WAREHOUSE_ID
        when 'W1' then 'North Hub'
        when 'W2' then 'South Hub'
        when 'W3' then 'East Hub'
        else 'Unknown'
    end,
    case WAREHOUSE_ID
        when 'W1' then 'NORTH'
        when 'W2' then 'SOUTH'
        when 'W3' then 'EAST'
        else 'UNKNOWN'
    end
from all_warehouses;


-- ############################################################
-- SECTION 4: LOAD FACT TABLE
-- ############################################################

-- Grain: 1 row per order line item (from STG_ORDER_ITEMS bridge)
-- Joins: order items + orders + best shipment + latest event + all dim keys
-- Best shipment = per order, prefer IS_VALID desc then earliest SHIPMENT_DATE
-- Latest event  = per order, most recent EVENT_TIMESTAMP
insert into CAPSTONE.CURATED.FACT_ORDER
    (ORDER_ITEM_ID, ORDER_ID, PRODUCT_KEY, CUSTOMER_KEY,
     ORDER_DATE_KEY, SHIPMENT_DATE_KEY, DELIVERY_DATE_KEY, WAREHOUSE_KEY,
     ORDER_STATUS, PAYMENT_METHOD, CHANNEL, SHIPMENT_STATUS,
     QUANTITY, UNIT_PRICE, LINE_TOTAL, ORDER_TOTAL,
     FULFILLMENT_DAYS, IS_FULFILLED, IS_VALID_SHIPMENT, SHIPMENT_DQ_REASON,
     LATEST_EVENT_TYPE, LATEST_EVENT_STATUS)
with best_shipment as (
    select *
    from (
        select
            s.*,
            row_number() over (
                partition by s.ORDER_ID
                order by s.IS_VALID desc, s.SHIPMENT_DATE asc nulls last
            ) as rn
        from CAPSTONE.STG.STG_SHIPMENTS s
    )
    where rn = 1
),
last_event as (
    select ORDER_ID, EVENT_TYPE, EVENT_STATUS
    from (
        select
            e.*,
            row_number() over (
                partition by e.ORDER_ID
                order by e.EVENT_TIMESTAMP desc
            ) as rn
        from CAPSTONE.STG.STG_EVENTS e
    )
    where rn = 1
)
select
    oi.ORDER_ITEM_ID,
    o.ORDER_ID,
    dp.DIM_PRODUCT_KEY,
    dc.DIM_CUSTOMER_KEY,
    to_number(to_char(o.ORDER_DATE,    'YYYYMMDD')),
    to_number(to_char(s.SHIPMENT_DATE, 'YYYYMMDD')),
    to_number(to_char(s.DELIVERY_DATE, 'YYYYMMDD')),
    dw.DIM_WAREHOUSE_KEY,
    o.ORDER_STATUS,
    o.PAYMENT_METHOD,
    o.CHANNEL,
    s.SHIPMENT_STATUS,
    oi.QUANTITY,
    oi.UNIT_PRICE,
    oi.LINE_TOTAL,
    o.TOTAL_AMOUNT,
    datediff('day', s.SHIPMENT_DATE, s.DELIVERY_DATE),
    s.SHIPMENT_STATUS = 'DELIVERED' and s.IS_VALID = true,
    s.IS_VALID,
    s.REJECTION_REASON,
    e.EVENT_TYPE,
    e.EVENT_STATUS
from CAPSTONE.STG.STG_ORDER_ITEMS oi
join CAPSTONE.STG.STG_ORDERS o
    on oi.ORDER_ID = o.ORDER_ID
left join CAPSTONE.CURATED.DIM_PRODUCT dp
    on oi.PRODUCT_ID = dp.PRODUCT_ID
left join CAPSTONE.CURATED.DIM_CUSTOMER dc
    on o.CUSTOMER_ID = dc.CUSTOMER_ID
left join best_shipment s
    on o.ORDER_ID = s.ORDER_ID
left join CAPSTONE.CURATED.DIM_WAREHOUSE dw
    on s.WAREHOUSE_ID = dw.WAREHOUSE_ID
left join last_event e
    on o.ORDER_ID = e.ORDER_ID;


-- ############################################################
-- SECTION 5: INVENTORY RECONCILIATION VIEW
-- ############################################################
-- Solves: Stockout detection, overstock identification,
--         demand vs supply mismatch, stale inventory alerting
--
-- RECONCILIATION_STATUS logic:
--   NO_INVENTORY_RECORD   = demand exists but no inventory row
--   DEMAND_EXCEEDS_STOCK  = ordered qty > current stock
--   BELOW_REORDER_LEVEL   = stock <= reorder level
--   OVERSTOCK             = stock > 3x reorder level
--   OK                    = healthy

create or replace view CAPSTONE.CURATED.V_INVENTORY_RECONCILIATION (
    PRODUCT_ID,
    WAREHOUSE_ID,
    STOCK_QUANTITY,
    REORDER_LEVEL,
    TOTAL_QTY_ORDERED,
    ORDER_LINE_COUNT,
    INVENTORY_LAST_UPDATED,
    LAST_ORDER_DATE,
    MISSING_FROM_INVENTORY,
    DEMAND_EXCEEDS_STOCK,
    STALE_INVENTORY,
    RECONCILIATION_STATUS
) as
with demand as (
    select
        dp.product_id,
        dw.warehouse_id,
        sum(f.quantity)    as total_qty_ordered,
        count(*)           as order_line_count,
        max(dd.full_date)  as last_order_date
    from CAPSTONE.CURATED.FACT_ORDER f
    join CAPSTONE.CURATED.DIM_PRODUCT dp
        on f.product_key = dp.dim_product_key
    left join CAPSTONE.CURATED.DIM_WAREHOUSE dw
        on f.warehouse_key = dw.dim_warehouse_key
    left join CAPSTONE.CURATED.DIM_DATE dd
        on f.order_date_key = dd.date_key
    group by dp.product_id, dw.warehouse_id
),
supply as (
    select
        product_id,
        warehouse_id,
        stock_quantity,
        reorder_level,
        last_updated
    from CAPSTONE.STG.STG_INVENTORY
)
select
    coalesce(d.product_id, s.product_id)       as product_id,
    coalesce(d.warehouse_id, s.warehouse_id)   as warehouse_id,
    coalesce(s.stock_quantity, 0)               as stock_quantity,
    coalesce(s.reorder_level, 0)               as reorder_level,
    coalesce(d.total_qty_ordered, 0)           as total_qty_ordered,
    coalesce(d.order_line_count, 0)            as order_line_count,
    s.last_updated                              as inventory_last_updated,
    d.last_order_date,

    d.product_id is not null and s.product_id is null
        as missing_from_inventory,

    s.stock_quantity is not null and d.total_qty_ordered > s.stock_quantity
        as demand_exceeds_stock,

    s.last_updated is not null and d.last_order_date is not null
        and datediff('day', s.last_updated, d.last_order_date) > 30
        as stale_inventory,

    case
        when s.product_id is null                    then 'NO_INVENTORY_RECORD'
        when d.total_qty_ordered > s.stock_quantity  then 'DEMAND_EXCEEDS_STOCK'
        when s.stock_quantity <= s.reorder_level     then 'BELOW_REORDER_LEVEL'
        when s.stock_quantity > 3 * s.reorder_level  then 'OVERSTOCK'
        else 'OK'
    end as reconciliation_status

from demand d
full outer join supply s
    on d.product_id = s.product_id
    and d.warehouse_id = s.warehouse_id;
