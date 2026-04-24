-- ============================================================
-- DAY 4: INCREMENTAL PIPELINE + AUTOMATION
-- ============================================================
-- Implements CDC-based incremental processing so that only
-- new/changed data flows through the pipeline.
--
-- Components:
--   Streams      -> Capture RAW table changes
--   Tracking     -> Remember affected order/product IDs
--   MERGE (RAW→STG)     -> Apply same DQ rules as Day 2
--   MERGE (STG→CURATED) -> Update dims + rebuild fact
--   Tasks        -> Automate on a 5-minute schedule
--
-- Both tasks run independently on a 5-minute schedule:
--   TASK_RAW_TO_STG_INCREMENTAL     has a WHEN condition
--   TASK_STG_TO_CURATED_INCREMENTAL runs unconditionally
-- ============================================================


-- ############################################################
-- SECTION 1: STREAMS (capture CDC on RAW tables)
-- ############################################################

create or replace stream CAPSTONE.CAPSTONE.RAW_ORDERS_STREAM
    on table CAPSTONE.CAPSTONE.RAW_ORDERS;

create or replace stream CAPSTONE.CAPSTONE.RAW_SHIPMENTS_STREAM
    on table CAPSTONE.CAPSTONE.RAW_SHIPMENTS;

create or replace stream CAPSTONE.CAPSTONE.RAW_INVENTORY_STREAM
    on table CAPSTONE.CAPSTONE.RAW_INVENTORY;

create or replace stream CAPSTONE.CAPSTONE.RAW_EVENTS_JSON_STREAM
    on table CAPSTONE.CAPSTONE.RAW_EVENTS_JSON;


-- ############################################################
-- SECTION 2: TRACKING TABLES
-- ############################################################

create or replace transient table CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS (
    ORDER_ID VARCHAR(255)
);

create or replace transient table CAPSTONE.STG.INCREMENTAL_CHANGED_PRODUCTS (
    PRODUCT_ID VARCHAR(255)
);


-- ############################################################
-- SECTION 3: RAW → STG INCREMENTAL MERGES (manual run)
-- ############################################################

-- 3A: MERGE Orders
merge into CAPSTONE.STG.STG_ORDERS tgt
using (
    with cleaned_orders as (
        select
            ORDER_ID,
            CUSTOMER_ID,
            try_to_date(ORDER_DATE, 'MM/DD/YYYY')   as ORDER_DATE,
            upper(trim(ORDER_STATUS))                as ORDER_STATUS,
            TOTAL_AMOUNT,
            upper(trim(PAYMENT_METHOD))              as PAYMENT_METHOD,
            lower(trim(CHANNEL))                     as CHANNEL,
            metadata$action                          as CDC_ACTION,
            metadata$isupdate                        as CDC_ISUPDATE,
            row_number() over (
                partition by ORDER_ID order by ORDER_ID
            ) as rn
        from CAPSTONE.CAPSTONE.RAW_ORDERS_STREAM
    )
    select *
    from cleaned_orders
    where rn = 1
      and CDC_ACTION in ('INSERT', 'DELETE')
      and ORDER_DATE is not null
      and TOTAL_AMOUNT > 0
      and ORDER_STATUS in ('PENDING','SHIPPED','DELIVERED','CANCELLED','RETURNED')
) src
on tgt.ORDER_ID = src.ORDER_ID
when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false
    then delete
when matched and src.CDC_ACTION = 'INSERT'
    then update set
        CUSTOMER_ID = src.CUSTOMER_ID, ORDER_DATE = src.ORDER_DATE,
        ORDER_STATUS = src.ORDER_STATUS, TOTAL_AMOUNT = src.TOTAL_AMOUNT,
        PAYMENT_METHOD = src.PAYMENT_METHOD, CHANNEL = src.CHANNEL
when not matched and src.CDC_ACTION = 'INSERT'
    then insert (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD, CHANNEL)
    values (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.ORDER_STATUS, src.TOTAL_AMOUNT, src.PAYMENT_METHOD, src.CHANNEL);

insert overwrite into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
select distinct ORDER_ID
from CAPSTONE.STG.STG_ORDERS
where ORDER_ID in (select distinct ORDER_ID from CAPSTONE.CAPSTONE.RAW_ORDERS);


-- 3B: MERGE Shipments (with DQ validation flags)
merge into CAPSTONE.STG.STG_SHIPMENTS tgt
using (
    with cleaned_shipments as (
        select
            SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID,
            try_to_date(SHIPMENT_DATE, 'MM/DD/YYYY')   as SHIPMENT_DATE,
            try_to_date(DELIVERY_DATE, 'MM/DD/YYYY')    as DELIVERY_DATE,
            upper(trim(SHIPMENT_STATUS))                 as SHIPMENT_STATUS,
            metadata$action                              as CDC_ACTION,
            metadata$isupdate                            as CDC_ISUPDATE,
            row_number() over (
                partition by SHIPMENT_ID order by SHIPMENT_ID
            ) as rn
        from CAPSTONE.CAPSTONE.RAW_SHIPMENTS_STREAM
    ),
    validated_shipments as (
        select
            SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID,
            SHIPMENT_DATE, DELIVERY_DATE, SHIPMENT_STATUS,
            CDC_ACTION, CDC_ISUPDATE,
            case
                when SHIPMENT_DATE is null or DELIVERY_DATE is null then false
                when DELIVERY_DATE < SHIPMENT_DATE then false
                when exists (
                    select 1 from CAPSTONE.STG.STG_ORDERS o
                    where o.ORDER_ID = cleaned_shipments.ORDER_ID
                      and cleaned_shipments.SHIPMENT_DATE < o.ORDER_DATE
                ) then false
                else true
            end as IS_VALID,
            case
                when SHIPMENT_DATE is null or DELIVERY_DATE is null then 'UNPARSEABLE_DATE'
                when DELIVERY_DATE < SHIPMENT_DATE then 'DELIVERY_BEFORE_SHIPMENT'
                when exists (
                    select 1 from CAPSTONE.STG.STG_ORDERS o
                    where o.ORDER_ID = cleaned_shipments.ORDER_ID
                      and cleaned_shipments.SHIPMENT_DATE < o.ORDER_DATE
                ) then 'SHIPMENT_BEFORE_ORDER'
                else null
            end as REJECTION_REASON
        from cleaned_shipments
        where rn = 1 and CDC_ACTION in ('INSERT', 'DELETE')
    )
    select * from validated_shipments
) src
on tgt.SHIPMENT_ID = src.SHIPMENT_ID
when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false
    then delete
when matched and src.CDC_ACTION = 'INSERT'
    then update set
        ORDER_ID = src.ORDER_ID, WAREHOUSE_ID = src.WAREHOUSE_ID,
        SHIPMENT_DATE = src.SHIPMENT_DATE, DELIVERY_DATE = src.DELIVERY_DATE,
        SHIPMENT_STATUS = src.SHIPMENT_STATUS,
        IS_VALID = src.IS_VALID, REJECTION_REASON = src.REJECTION_REASON
when not matched and src.CDC_ACTION = 'INSERT'
    then insert (SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID, SHIPMENT_DATE, DELIVERY_DATE, SHIPMENT_STATUS, IS_VALID, REJECTION_REASON)
    values (src.SHIPMENT_ID, src.ORDER_ID, src.WAREHOUSE_ID, src.SHIPMENT_DATE, src.DELIVERY_DATE, src.SHIPMENT_STATUS, src.IS_VALID, src.REJECTION_REASON);

insert into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
select distinct ORDER_ID from CAPSTONE.STG.STG_SHIPMENTS
where ORDER_ID is not null
  and ORDER_ID not in (select ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);


-- 3C: MERGE Inventory
merge into CAPSTONE.STG.STG_INVENTORY tgt
using (
    with cleaned_inventory as (
        select
            INVENTORY_ID, PRODUCT_ID, WAREHOUSE_ID,
            STOCK_QUANTITY, REORDER_LEVEL, LAST_UPDATED,
            metadata$action as CDC_ACTION,
            metadata$isupdate as CDC_ISUPDATE,
            row_number() over (
                partition by INVENTORY_ID order by INVENTORY_ID
            ) as rn
        from CAPSTONE.CAPSTONE.RAW_INVENTORY_STREAM
    )
    select * from cleaned_inventory
    where rn = 1
      and CDC_ACTION in ('INSERT', 'DELETE')
      and PRODUCT_ID is not null
      and STOCK_QUANTITY >= 0
      and REORDER_LEVEL >= 0
) src
on tgt.INVENTORY_ID = src.INVENTORY_ID
when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false
    then delete
when matched and src.CDC_ACTION = 'INSERT'
    then update set
        PRODUCT_ID = src.PRODUCT_ID, WAREHOUSE_ID = src.WAREHOUSE_ID,
        STOCK_QUANTITY = src.STOCK_QUANTITY, REORDER_LEVEL = src.REORDER_LEVEL,
        LAST_UPDATED = src.LAST_UPDATED
when not matched and src.CDC_ACTION = 'INSERT'
    then insert (INVENTORY_ID, PRODUCT_ID, WAREHOUSE_ID, STOCK_QUANTITY, REORDER_LEVEL, LAST_UPDATED)
    values (src.INVENTORY_ID, src.PRODUCT_ID, src.WAREHOUSE_ID, src.STOCK_QUANTITY, src.REORDER_LEVEL, src.LAST_UPDATED);

insert overwrite into CAPSTONE.STG.INCREMENTAL_CHANGED_PRODUCTS
select distinct PRODUCT_ID from CAPSTONE.STG.STG_INVENTORY where PRODUCT_ID is not null;


-- 3D: MERGE Events (JSON flattening)
merge into CAPSTONE.STG.STG_EVENTS tgt
using (
    with cleaned_events as (
        select
            RAW:event_id::varchar                              as EVENT_ID,
            RAW:order_id::varchar                              as ORDER_ID,
            upper(trim(RAW:event_type::varchar))               as EVENT_TYPE,
            try_to_timestamp_ntz(RAW:event_timestamp::varchar) as EVENT_TIMESTAMP,
            upper(trim(RAW:event_status::varchar))             as EVENT_STATUS,
            metadata$action                                    as CDC_ACTION,
            metadata$isupdate                                  as CDC_ISUPDATE,
            row_number() over (
                partition by RAW:event_id::varchar order by RAW:event_id::varchar
            ) as rn
        from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON_STREAM
    )
    select * from cleaned_events
    where rn = 1
      and CDC_ACTION in ('INSERT', 'DELETE')
      and EVENT_ID is not null
      and ORDER_ID is not null
      and EVENT_TIMESTAMP is not null
      and EVENT_STATUS in ('SUCCESS', 'FAILED')
) src
on tgt.EVENT_ID = src.EVENT_ID
when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false
    then delete
when matched and src.CDC_ACTION = 'INSERT'
    then update set
        ORDER_ID = src.ORDER_ID, EVENT_TYPE = src.EVENT_TYPE,
        EVENT_TIMESTAMP = src.EVENT_TIMESTAMP, EVENT_STATUS = src.EVENT_STATUS
when not matched and src.CDC_ACTION = 'INSERT'
    then insert (EVENT_ID, ORDER_ID, EVENT_TYPE, EVENT_TIMESTAMP, EVENT_STATUS)
    values (src.EVENT_ID, src.ORDER_ID, src.EVENT_TYPE, src.EVENT_TIMESTAMP, src.EVENT_STATUS);

insert into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
select distinct ORDER_ID from CAPSTONE.STG.STG_EVENTS
where ORDER_ID is not null
  and ORDER_ID not in (select ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);


-- ############################################################
-- SECTION 4: STG → CURATED INCREMENTAL MERGES (manual run)
-- ############################################################

-- 4A: DIM_CUSTOMER
merge into CAPSTONE.CURATED.DIM_CUSTOMER tgt
using CAPSTONE.STG.STG_CUSTOMERS src
on tgt.CUSTOMER_ID = src.CUSTOMER_ID
when matched then update set
    CUSTOMER_NAME = src.CUSTOMER_NAME, EMAIL = src.EMAIL,
    PHONE = src.PHONE, CITY = src.CITY, STATE = src.STATE,
    REGION = src.REGION, CUSTOMER_SEGMENT = src.CUSTOMER_SEGMENT
when not matched then insert
    (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY, STATE, REGION, CUSTOMER_SEGMENT)
values
    (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.EMAIL, src.PHONE, src.CITY, src.STATE, src.REGION, src.CUSTOMER_SEGMENT);

-- 4B: DIM_PRODUCT (enriched with inventory)
merge into CAPSTONE.CURATED.DIM_PRODUCT tgt
using (
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
        p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY, p.SUB_CATEGORY, p.PRICE,
        coalesce(i.TOTAL_STOCK_QUANTITY, 0) as TOTAL_STOCK_QUANTITY,
        coalesce(i.AVG_REORDER_LEVEL, 0)   as AVG_REORDER_LEVEL,
        coalesce(i.WAREHOUSE_COUNT, 0)     as WAREHOUSE_COUNT,
        i.TOTAL_STOCK_QUANTITY is not null
            and i.TOTAL_STOCK_QUANTITY <= i.TOTAL_REORDER_LEVEL as IS_LOW_STOCK,
        i.TOTAL_STOCK_QUANTITY is not null
            and i.TOTAL_STOCK_QUANTITY > 3 * i.TOTAL_REORDER_LEVEL as IS_OVERSTOCK
    from CAPSTONE.STG.STG_PRODUCTS p
    left join inventory_summary i on p.PRODUCT_ID = i.PRODUCT_ID
) src
on tgt.PRODUCT_ID = src.PRODUCT_ID
when matched then update set
    PRODUCT_NAME = src.PRODUCT_NAME, CATEGORY = src.CATEGORY,
    SUB_CATEGORY = src.SUB_CATEGORY, PRICE = src.PRICE,
    TOTAL_STOCK_QUANTITY = src.TOTAL_STOCK_QUANTITY,
    AVG_REORDER_LEVEL = src.AVG_REORDER_LEVEL,
    WAREHOUSE_COUNT = src.WAREHOUSE_COUNT,
    IS_LOW_STOCK = src.IS_LOW_STOCK, IS_OVERSTOCK = src.IS_OVERSTOCK
when not matched then insert
    (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, PRICE,
     TOTAL_STOCK_QUANTITY, AVG_REORDER_LEVEL, WAREHOUSE_COUNT, IS_LOW_STOCK, IS_OVERSTOCK)
values
    (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.SUB_CATEGORY, src.PRICE,
     src.TOTAL_STOCK_QUANTITY, src.AVG_REORDER_LEVEL, src.WAREHOUSE_COUNT, src.IS_LOW_STOCK, src.IS_OVERSTOCK);

-- 4C: DIM_WAREHOUSE
merge into CAPSTONE.CURATED.DIM_WAREHOUSE tgt
using (
    with all_warehouses as (
        select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_SHIPMENTS
        union
        select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_INVENTORY
    )
    select
        WAREHOUSE_ID,
        case WAREHOUSE_ID when 'W1' then 'North Hub' when 'W2' then 'South Hub' when 'W3' then 'East Hub' else 'Unknown' end as WAREHOUSE_NAME,
        case WAREHOUSE_ID when 'W1' then 'NORTH' when 'W2' then 'SOUTH' when 'W3' then 'EAST' else 'UNKNOWN' end as REGION
    from all_warehouses
) src
on tgt.WAREHOUSE_ID = src.WAREHOUSE_ID
when matched then update set WAREHOUSE_NAME = src.WAREHOUSE_NAME, REGION = src.REGION
when not matched then insert (WAREHOUSE_ID, WAREHOUSE_NAME, REGION)
    values (src.WAREHOUSE_ID, src.WAREHOUSE_NAME, src.REGION);

-- 4D: FACT_ORDER (delete + reinsert for changed orders only)
delete from CAPSTONE.CURATED.FACT_ORDER
where ORDER_ID in (select distinct ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);

insert into CAPSTONE.CURATED.FACT_ORDER
    (ORDER_ITEM_ID, ORDER_ID, PRODUCT_KEY, CUSTOMER_KEY,
     ORDER_DATE_KEY, SHIPMENT_DATE_KEY, DELIVERY_DATE_KEY, WAREHOUSE_KEY,
     ORDER_STATUS, PAYMENT_METHOD, CHANNEL, SHIPMENT_STATUS,
     QUANTITY, UNIT_PRICE, LINE_TOTAL, ORDER_TOTAL,
     FULFILLMENT_DAYS, IS_FULFILLED, IS_VALID_SHIPMENT, SHIPMENT_DQ_REASON,
     LATEST_EVENT_TYPE, LATEST_EVENT_STATUS)
with impacted_orders as (
    select distinct ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
),
best_shipment as (
    select * from (
        select s.*,
            row_number() over (
                partition by s.ORDER_ID
                order by s.IS_VALID desc, s.SHIPMENT_DATE asc nulls last
            ) as rn
        from CAPSTONE.STG.STG_SHIPMENTS s
        join impacted_orders io on s.ORDER_ID = io.ORDER_ID
    ) where rn = 1
),
last_event as (
    select ORDER_ID, EVENT_TYPE, EVENT_STATUS from (
        select e.*,
            row_number() over (
                partition by e.ORDER_ID
                order by e.EVENT_TIMESTAMP desc
            ) as rn
        from CAPSTONE.STG.STG_EVENTS e
        join impacted_orders io on e.ORDER_ID = io.ORDER_ID
    ) where rn = 1
)
select
    oi.ORDER_ITEM_ID, o.ORDER_ID,
    dp.DIM_PRODUCT_KEY, dc.DIM_CUSTOMER_KEY,
    to_number(to_char(o.ORDER_DATE, 'YYYYMMDD')),
    to_number(to_char(s.SHIPMENT_DATE, 'YYYYMMDD')),
    to_number(to_char(s.DELIVERY_DATE, 'YYYYMMDD')),
    dw.DIM_WAREHOUSE_KEY,
    o.ORDER_STATUS, o.PAYMENT_METHOD, o.CHANNEL, s.SHIPMENT_STATUS,
    oi.QUANTITY, oi.UNIT_PRICE, oi.LINE_TOTAL, o.TOTAL_AMOUNT,
    datediff('day', s.SHIPMENT_DATE, s.DELIVERY_DATE),
    s.SHIPMENT_STATUS = 'DELIVERED' and s.IS_VALID = true,
    s.IS_VALID, s.REJECTION_REASON,
    e.EVENT_TYPE, e.EVENT_STATUS
from CAPSTONE.STG.STG_ORDER_ITEMS oi
join CAPSTONE.STG.STG_ORDERS o on oi.ORDER_ID = o.ORDER_ID
join impacted_orders io on o.ORDER_ID = io.ORDER_ID
left join CAPSTONE.CURATED.DIM_PRODUCT dp on oi.PRODUCT_ID = dp.PRODUCT_ID
left join CAPSTONE.CURATED.DIM_CUSTOMER dc on o.CUSTOMER_ID = dc.CUSTOMER_ID
left join best_shipment s on o.ORDER_ID = s.ORDER_ID
left join CAPSTONE.CURATED.DIM_WAREHOUSE dw on s.WAREHOUSE_ID = dw.WAREHOUSE_ID
left join last_event e on o.ORDER_ID = e.ORDER_ID;


-- ############################################################
-- SECTION 5: TASK AUTOMATION
-- ############################################################
-- Both tasks are INDEPENDENT (both have schedule, no predecessor).
-- Task 1 only fires when streams have data (WHEN condition).
-- Task 2 runs every 5 minutes unconditionally.
-- ############################################################

-- 5A: Task 1 — RAW to STG (fires only when streams have data)
create or replace task CAPSTONE.CURATED.TASK_RAW_TO_STG_INCREMENTAL
    warehouse = COMPUTE_WH
    schedule  = '5 minute'
    when
        system$stream_has_data('CAPSTONE.CAPSTONE.RAW_ORDERS_STREAM')
        or system$stream_has_data('CAPSTONE.CAPSTONE.RAW_SHIPMENTS_STREAM')
        or system$stream_has_data('CAPSTONE.CAPSTONE.RAW_INVENTORY_STREAM')
        or system$stream_has_data('CAPSTONE.CAPSTONE.RAW_EVENTS_JSON_STREAM')
as
begin
    create or replace transient table CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS (
        order_id varchar(255)
    );
    create or replace transient table CAPSTONE.STG.INCREMENTAL_CHANGED_PRODUCTS (
        product_id varchar(255)
    );

    merge into CAPSTONE.STG.STG_ORDERS tgt
    using (
        with cleaned_orders as (
            select ORDER_ID, CUSTOMER_ID,
                try_to_date(ORDER_DATE, 'MM/DD/YYYY') as ORDER_DATE,
                upper(trim(ORDER_STATUS)) as ORDER_STATUS,
                TOTAL_AMOUNT,
                upper(trim(PAYMENT_METHOD)) as PAYMENT_METHOD,
                lower(trim(CHANNEL)) as CHANNEL,
                metadata$action as CDC_ACTION,
                metadata$isupdate as CDC_ISUPDATE,
                row_number() over (partition by ORDER_ID order by ORDER_ID) as rn
            from CAPSTONE.CAPSTONE.RAW_ORDERS_STREAM
        )
        select * from cleaned_orders
        where rn = 1 and CDC_ACTION in ('INSERT', 'DELETE')
          and ORDER_DATE is not null and TOTAL_AMOUNT > 0
          and ORDER_STATUS in ('PENDING','SHIPPED','DELIVERED','CANCELLED','RETURNED')
    ) src
    on tgt.ORDER_ID = src.ORDER_ID
    when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false then delete
    when matched and src.CDC_ACTION = 'INSERT' then update set
        CUSTOMER_ID = src.CUSTOMER_ID, ORDER_DATE = src.ORDER_DATE,
        ORDER_STATUS = src.ORDER_STATUS, TOTAL_AMOUNT = src.TOTAL_AMOUNT,
        PAYMENT_METHOD = src.PAYMENT_METHOD, CHANNEL = src.CHANNEL
    when not matched and src.CDC_ACTION = 'INSERT' then insert
        (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT, PAYMENT_METHOD, CHANNEL)
    values
        (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.ORDER_STATUS, src.TOTAL_AMOUNT, src.PAYMENT_METHOD, src.CHANNEL);

    insert overwrite into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
    select distinct ORDER_ID from CAPSTONE.STG.STG_ORDERS
    where ORDER_ID in (select distinct ORDER_ID from CAPSTONE.CAPSTONE.RAW_ORDERS);

    merge into CAPSTONE.STG.STG_SHIPMENTS tgt
    using (
        with cleaned_shipments as (
            select SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID,
                try_to_date(SHIPMENT_DATE, 'MM/DD/YYYY') as SHIPMENT_DATE,
                try_to_date(DELIVERY_DATE, 'MM/DD/YYYY') as DELIVERY_DATE,
                upper(trim(SHIPMENT_STATUS)) as SHIPMENT_STATUS,
                metadata$action as CDC_ACTION, metadata$isupdate as CDC_ISUPDATE,
                row_number() over (partition by SHIPMENT_ID order by SHIPMENT_ID) as rn
            from CAPSTONE.CAPSTONE.RAW_SHIPMENTS_STREAM
        ),
        validated_shipments as (
            select
                SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID,
                SHIPMENT_DATE, DELIVERY_DATE, SHIPMENT_STATUS,
                CDC_ACTION, CDC_ISUPDATE,
                case
                    when SHIPMENT_DATE is null or DELIVERY_DATE is null then false
                    when DELIVERY_DATE < SHIPMENT_DATE then false
                    when exists (
                        select 1 from CAPSTONE.STG.STG_ORDERS o
                        where o.ORDER_ID = cleaned_shipments.ORDER_ID
                          and cleaned_shipments.SHIPMENT_DATE < o.ORDER_DATE
                    ) then false
                    else true
                end as IS_VALID,
                case
                    when SHIPMENT_DATE is null or DELIVERY_DATE is null then 'UNPARSEABLE_DATE'
                    when DELIVERY_DATE < SHIPMENT_DATE then 'DELIVERY_BEFORE_SHIPMENT'
                    when exists (
                        select 1 from CAPSTONE.STG.STG_ORDERS o
                        where o.ORDER_ID = cleaned_shipments.ORDER_ID
                          and cleaned_shipments.SHIPMENT_DATE < o.ORDER_DATE
                    ) then 'SHIPMENT_BEFORE_ORDER'
                    else null
                end as REJECTION_REASON
            from cleaned_shipments
            where rn = 1 and CDC_ACTION in ('INSERT', 'DELETE')
        )
        select * from validated_shipments
    ) src
    on tgt.SHIPMENT_ID = src.SHIPMENT_ID
    when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false then delete
    when matched and src.CDC_ACTION = 'INSERT' then update set
        ORDER_ID = src.ORDER_ID, WAREHOUSE_ID = src.WAREHOUSE_ID,
        SHIPMENT_DATE = src.SHIPMENT_DATE, DELIVERY_DATE = src.DELIVERY_DATE,
        SHIPMENT_STATUS = src.SHIPMENT_STATUS,
        IS_VALID = src.IS_VALID, REJECTION_REASON = src.REJECTION_REASON
    when not matched and src.CDC_ACTION = 'INSERT' then insert
        (SHIPMENT_ID, ORDER_ID, WAREHOUSE_ID, SHIPMENT_DATE, DELIVERY_DATE, SHIPMENT_STATUS, IS_VALID, REJECTION_REASON)
    values
        (src.SHIPMENT_ID, src.ORDER_ID, src.WAREHOUSE_ID, src.SHIPMENT_DATE, src.DELIVERY_DATE, src.SHIPMENT_STATUS, src.IS_VALID, src.REJECTION_REASON);

    insert into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
    select distinct ORDER_ID from CAPSTONE.STG.STG_SHIPMENTS
    where ORDER_ID is not null
      and ORDER_ID not in (select ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);

    merge into CAPSTONE.STG.STG_INVENTORY tgt
    using (
        with cleaned_inventory as (
            select
                INVENTORY_ID, PRODUCT_ID, WAREHOUSE_ID,
                STOCK_QUANTITY, REORDER_LEVEL, LAST_UPDATED,
                metadata$action as CDC_ACTION, metadata$isupdate as CDC_ISUPDATE,
                row_number() over (partition by INVENTORY_ID order by INVENTORY_ID) as rn
            from CAPSTONE.CAPSTONE.RAW_INVENTORY_STREAM
        )
        select * from cleaned_inventory
        where rn = 1 and CDC_ACTION in ('INSERT', 'DELETE')
          and PRODUCT_ID is not null and STOCK_QUANTITY >= 0 and REORDER_LEVEL >= 0
    ) src
    on tgt.INVENTORY_ID = src.INVENTORY_ID
    when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false then delete
    when matched and src.CDC_ACTION = 'INSERT' then update set
        PRODUCT_ID = src.PRODUCT_ID, WAREHOUSE_ID = src.WAREHOUSE_ID,
        STOCK_QUANTITY = src.STOCK_QUANTITY, REORDER_LEVEL = src.REORDER_LEVEL,
        LAST_UPDATED = src.LAST_UPDATED
    when not matched and src.CDC_ACTION = 'INSERT' then insert
        (INVENTORY_ID, PRODUCT_ID, WAREHOUSE_ID, STOCK_QUANTITY, REORDER_LEVEL, LAST_UPDATED)
    values
        (src.INVENTORY_ID, src.PRODUCT_ID, src.WAREHOUSE_ID, src.STOCK_QUANTITY, src.REORDER_LEVEL, src.LAST_UPDATED);

    insert overwrite into CAPSTONE.STG.INCREMENTAL_CHANGED_PRODUCTS
    select distinct PRODUCT_ID from CAPSTONE.STG.STG_INVENTORY where PRODUCT_ID is not null;

    merge into CAPSTONE.STG.STG_EVENTS tgt
    using (
        with cleaned_events as (
            select
                RAW:event_id::varchar as EVENT_ID,
                RAW:order_id::varchar as ORDER_ID,
                upper(trim(RAW:event_type::varchar)) as EVENT_TYPE,
                try_to_timestamp_ntz(RAW:event_timestamp::varchar) as EVENT_TIMESTAMP,
                upper(trim(RAW:event_status::varchar)) as EVENT_STATUS,
                metadata$action as CDC_ACTION, metadata$isupdate as CDC_ISUPDATE,
                row_number() over (partition by RAW:event_id::varchar order by RAW:event_id::varchar) as rn
            from CAPSTONE.CAPSTONE.RAW_EVENTS_JSON_STREAM
        )
        select * from cleaned_events
        where rn = 1 and CDC_ACTION in ('INSERT', 'DELETE')
          and EVENT_ID is not null and ORDER_ID is not null
          and EVENT_TIMESTAMP is not null and EVENT_STATUS in ('SUCCESS', 'FAILED')
    ) src
    on tgt.EVENT_ID = src.EVENT_ID
    when matched and src.CDC_ACTION = 'DELETE' and src.CDC_ISUPDATE = false then delete
    when matched and src.CDC_ACTION = 'INSERT' then update set
        ORDER_ID = src.ORDER_ID, EVENT_TYPE = src.EVENT_TYPE,
        EVENT_TIMESTAMP = src.EVENT_TIMESTAMP, EVENT_STATUS = src.EVENT_STATUS
    when not matched and src.CDC_ACTION = 'INSERT' then insert
        (EVENT_ID, ORDER_ID, EVENT_TYPE, EVENT_TIMESTAMP, EVENT_STATUS)
    values
        (src.EVENT_ID, src.ORDER_ID, src.EVENT_TYPE, src.EVENT_TIMESTAMP, src.EVENT_STATUS);

    insert into CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
    select distinct ORDER_ID from CAPSTONE.STG.STG_EVENTS
    where ORDER_ID is not null
      and ORDER_ID not in (select ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);
end;


-- 5B: Task 2 — STG to CURATED (runs every 5 min, no WHEN condition)
create or replace task CAPSTONE.CURATED.TASK_STG_TO_CURATED_INCREMENTAL
    warehouse = COMPUTE_WH
    schedule  = '5 minute'
as
begin
    merge into CAPSTONE.CURATED.DIM_CUSTOMER tgt
    using CAPSTONE.STG.STG_CUSTOMERS src
    on tgt.CUSTOMER_ID = src.CUSTOMER_ID
    when matched then update set
        CUSTOMER_NAME = src.CUSTOMER_NAME, EMAIL = src.EMAIL,
        PHONE = src.PHONE, CITY = src.CITY, STATE = src.STATE,
        REGION = src.REGION, CUSTOMER_SEGMENT = src.CUSTOMER_SEGMENT
    when not matched then insert
        (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, PHONE, CITY, STATE, REGION, CUSTOMER_SEGMENT)
    values
        (src.CUSTOMER_ID, src.CUSTOMER_NAME, src.EMAIL, src.PHONE, src.CITY, src.STATE, src.REGION, src.CUSTOMER_SEGMENT);

    merge into CAPSTONE.CURATED.DIM_PRODUCT tgt
    using (
        with inventory_summary as (
            select PRODUCT_ID,
                sum(STOCK_QUANTITY) as TOTAL_STOCK_QUANTITY,
                round(avg(REORDER_LEVEL), 0) as AVG_REORDER_LEVEL,
                count(distinct WAREHOUSE_ID) as WAREHOUSE_COUNT,
                sum(REORDER_LEVEL) as TOTAL_REORDER_LEVEL
            from CAPSTONE.STG.STG_INVENTORY group by PRODUCT_ID
        )
        select p.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY, p.SUB_CATEGORY, p.PRICE,
            coalesce(i.TOTAL_STOCK_QUANTITY, 0) as TOTAL_STOCK_QUANTITY,
            coalesce(i.AVG_REORDER_LEVEL, 0) as AVG_REORDER_LEVEL,
            coalesce(i.WAREHOUSE_COUNT, 0) as WAREHOUSE_COUNT,
            i.TOTAL_STOCK_QUANTITY is not null and i.TOTAL_STOCK_QUANTITY <= i.TOTAL_REORDER_LEVEL as IS_LOW_STOCK,
            i.TOTAL_STOCK_QUANTITY is not null and i.TOTAL_STOCK_QUANTITY > 3 * i.TOTAL_REORDER_LEVEL as IS_OVERSTOCK
        from CAPSTONE.STG.STG_PRODUCTS p
        left join inventory_summary i on p.PRODUCT_ID = i.PRODUCT_ID
    ) src
    on tgt.PRODUCT_ID = src.PRODUCT_ID
    when matched then update set
        PRODUCT_NAME = src.PRODUCT_NAME, CATEGORY = src.CATEGORY,
        SUB_CATEGORY = src.SUB_CATEGORY, PRICE = src.PRICE,
        TOTAL_STOCK_QUANTITY = src.TOTAL_STOCK_QUANTITY,
        AVG_REORDER_LEVEL = src.AVG_REORDER_LEVEL,
        WAREHOUSE_COUNT = src.WAREHOUSE_COUNT,
        IS_LOW_STOCK = src.IS_LOW_STOCK, IS_OVERSTOCK = src.IS_OVERSTOCK
    when not matched then insert
        (PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, PRICE,
         TOTAL_STOCK_QUANTITY, AVG_REORDER_LEVEL, WAREHOUSE_COUNT, IS_LOW_STOCK, IS_OVERSTOCK)
    values
        (src.PRODUCT_ID, src.PRODUCT_NAME, src.CATEGORY, src.SUB_CATEGORY, src.PRICE,
         src.TOTAL_STOCK_QUANTITY, src.AVG_REORDER_LEVEL, src.WAREHOUSE_COUNT, src.IS_LOW_STOCK, src.IS_OVERSTOCK);

    merge into CAPSTONE.CURATED.DIM_WAREHOUSE tgt
    using (
        with wh as (
            select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_SHIPMENTS
            union
            select distinct WAREHOUSE_ID from CAPSTONE.STG.STG_INVENTORY
        )
        select WAREHOUSE_ID,
            case WAREHOUSE_ID when 'W1' then 'North Hub' when 'W2' then 'South Hub' when 'W3' then 'East Hub' else 'Unknown' end as WAREHOUSE_NAME,
            case WAREHOUSE_ID when 'W1' then 'NORTH' when 'W2' then 'SOUTH' when 'W3' then 'EAST' else 'UNKNOWN' end as REGION
        from wh
    ) src
    on tgt.WAREHOUSE_ID = src.WAREHOUSE_ID
    when matched then update set WAREHOUSE_NAME = src.WAREHOUSE_NAME, REGION = src.REGION
    when not matched then insert (WAREHOUSE_ID, WAREHOUSE_NAME, REGION)
        values (src.WAREHOUSE_ID, src.WAREHOUSE_NAME, src.REGION);

    delete from CAPSTONE.CURATED.FACT_ORDER
    where ORDER_ID in (select distinct ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS);

    insert into CAPSTONE.CURATED.FACT_ORDER
        (ORDER_ITEM_ID, ORDER_ID, PRODUCT_KEY, CUSTOMER_KEY,
         ORDER_DATE_KEY, SHIPMENT_DATE_KEY, DELIVERY_DATE_KEY, WAREHOUSE_KEY,
         ORDER_STATUS, PAYMENT_METHOD, CHANNEL, SHIPMENT_STATUS,
         QUANTITY, UNIT_PRICE, LINE_TOTAL, ORDER_TOTAL,
         FULFILLMENT_DAYS, IS_FULFILLED, IS_VALID_SHIPMENT, SHIPMENT_DQ_REASON,
         LATEST_EVENT_TYPE, LATEST_EVENT_STATUS)
    with impacted_orders as (
        select distinct ORDER_ID from CAPSTONE.STG.INCREMENTAL_CHANGED_ORDERS
    ),
    best_shipment as (
        select * from (
            select s.*, row_number() over (
                partition by s.ORDER_ID order by s.IS_VALID desc, s.SHIPMENT_DATE asc nulls last
            ) as rn
            from CAPSTONE.STG.STG_SHIPMENTS s
            join impacted_orders io on s.ORDER_ID = io.ORDER_ID
        ) where rn = 1
    ),
    last_event as (
        select ORDER_ID, EVENT_TYPE, EVENT_STATUS from (
            select e.*, row_number() over (
                partition by e.ORDER_ID order by e.EVENT_TIMESTAMP desc
            ) as rn
            from CAPSTONE.STG.STG_EVENTS e
            join impacted_orders io on e.ORDER_ID = io.ORDER_ID
        ) where rn = 1
    )
    select
        oi.ORDER_ITEM_ID, o.ORDER_ID, dp.DIM_PRODUCT_KEY, dc.DIM_CUSTOMER_KEY,
        to_number(to_char(o.ORDER_DATE, 'YYYYMMDD')),
        to_number(to_char(s.SHIPMENT_DATE, 'YYYYMMDD')),
        to_number(to_char(s.DELIVERY_DATE, 'YYYYMMDD')),
        dw.DIM_WAREHOUSE_KEY,
        o.ORDER_STATUS, o.PAYMENT_METHOD, o.CHANNEL, s.SHIPMENT_STATUS,
        oi.QUANTITY, oi.UNIT_PRICE, oi.LINE_TOTAL, o.TOTAL_AMOUNT,
        datediff('day', s.SHIPMENT_DATE, s.DELIVERY_DATE),
        s.SHIPMENT_STATUS = 'DELIVERED' and s.IS_VALID = true,
        s.IS_VALID, s.REJECTION_REASON,
        e.EVENT_TYPE, e.EVENT_STATUS
    from CAPSTONE.STG.STG_ORDER_ITEMS oi
    join CAPSTONE.STG.STG_ORDERS o on oi.ORDER_ID = o.ORDER_ID
    join impacted_orders io on o.ORDER_ID = io.ORDER_ID
    left join CAPSTONE.CURATED.DIM_PRODUCT dp on oi.PRODUCT_ID = dp.PRODUCT_ID
    left join CAPSTONE.CURATED.DIM_CUSTOMER dc on o.CUSTOMER_ID = dc.CUSTOMER_ID
    left join best_shipment s on o.ORDER_ID = s.ORDER_ID
    left join CAPSTONE.CURATED.DIM_WAREHOUSE dw on s.WAREHOUSE_ID = dw.WAREHOUSE_ID
    left join last_event e on o.ORDER_ID = e.ORDER_ID;
end;


-- 5C: Both tasks created suspended
alter task CAPSTONE.CURATED.TASK_RAW_TO_STG_INCREMENTAL suspend;
alter task CAPSTONE.CURATED.TASK_STG_TO_CURATED_INCREMENTAL suspend;

-- To activate:
--   ALTER TASK CAPSTONE.CURATED.TASK_STG_TO_CURATED_INCREMENTAL RESUME;
--   ALTER TASK CAPSTONE.CURATED.TASK_RAW_TO_STG_INCREMENTAL RESUME;
