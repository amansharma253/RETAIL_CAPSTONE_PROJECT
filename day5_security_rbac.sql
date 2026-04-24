-- ============================================================
-- DAY 5: RBAC + MASKING POLICIES
-- ============================================================
-- Creates custom roles, role hierarchy, masking policies
-- (EMAIL_MASK, PHONE_MASK), and all privilege grants for
-- the CAPSTONE database.
--
-- Role Design:
--   CAPSTONE_ADMIN      -> Full read access to all schemas
--   CAPSTONE_ANALYST    -> Read access to CURATED only
--   CAPSTONE_OPERATIONS -> Read access to STG + CURATED
--   All three roll up to SYSADMIN
-- ============================================================


-- ############################################################
-- SECTION 1: CREATE ROLES
-- ############################################################

create role if not exists CAPSTONE_ADMIN;
create role if not exists CAPSTONE_ANALYST;
create role if not exists CAPSTONE_OPERATIONS;

-- Role hierarchy: all custom roles -> SYSADMIN
grant role CAPSTONE_ADMIN      to role SYSADMIN;
grant role CAPSTONE_ANALYST    to role SYSADMIN;
grant role CAPSTONE_OPERATIONS to role SYSADMIN;


-- ############################################################
-- SECTION 2: MASKING POLICIES
-- ############################################################

-- EMAIL_MASK: Full access for ADMIN, partial for ANALYST/OPERATIONS, masked for others
create or replace masking policy CAPSTONE.CURATED.EMAIL_MASK
    as (VAL VARCHAR) returns VARCHAR ->
    case
        when current_role() in ('ACCOUNTADMIN', 'CAPSTONE_ADMIN') then val
        when current_role() in ('CAPSTONE_ANALYST', 'CAPSTONE_OPERATIONS') then regexp_replace(val, '.+\\@', '*****@')
        else '********'
    end;

-- PHONE_MASK: Full access for ADMIN, masked for all others
create or replace masking policy CAPSTONE.CURATED.PHONE_MASK
    as (VAL VARCHAR) returns VARCHAR ->
    case
        when current_role() in ('ACCOUNTADMIN', 'CAPSTONE_ADMIN') then val
        else 'XXX-XXX-XXXX'
    end;

-- Apply masking policies to DIM_CUSTOMER
alter table CAPSTONE.CURATED.DIM_CUSTOMER
    modify column EMAIL set masking policy CAPSTONE.CURATED.EMAIL_MASK;

alter table CAPSTONE.CURATED.DIM_CUSTOMER
    modify column PHONE set masking policy CAPSTONE.CURATED.PHONE_MASK;


-- ############################################################
-- SECTION 3: GRANTS - CAPSTONE_ADMIN
-- ############################################################
-- Full SELECT on all schemas (CAPSTONE, STG, CURATED) + views

grant usage on database CAPSTONE to role CAPSTONE_ADMIN;
grant usage on schema CAPSTONE.CAPSTONE to role CAPSTONE_ADMIN;
grant usage on schema CAPSTONE.STG      to role CAPSTONE_ADMIN;
grant usage on schema CAPSTONE.CURATED  to role CAPSTONE_ADMIN;
grant usage on warehouse COMPUTE_WH     to role CAPSTONE_ADMIN;

grant select on all tables in schema CAPSTONE.CAPSTONE to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE.STG      to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE.CURATED  to role CAPSTONE_ADMIN;
grant select on all views  in schema CAPSTONE.CURATED  to role CAPSTONE_ADMIN;


-- ############################################################
-- SECTION 4: GRANTS - CAPSTONE_ANALYST
-- ############################################################
-- Read-only access to CURATED schema only

grant usage on database CAPSTONE         to role CAPSTONE_ANALYST;
grant usage on schema CAPSTONE.CURATED   to role CAPSTONE_ANALYST;
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_ANALYST;

grant select on all tables in schema CAPSTONE.CURATED to role CAPSTONE_ANALYST;
grant select on all views  in schema CAPSTONE.CURATED to role CAPSTONE_ANALYST;


-- ############################################################
-- SECTION 5: GRANTS - CAPSTONE_OPERATIONS
-- ############################################################
-- Read access to STG + CURATED schemas

grant usage on database CAPSTONE         to role CAPSTONE_OPERATIONS;
grant usage on schema CAPSTONE.STG       to role CAPSTONE_OPERATIONS;
grant usage on schema CAPSTONE.CURATED   to role CAPSTONE_OPERATIONS;
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_OPERATIONS;

grant select on all tables in schema CAPSTONE.STG     to role CAPSTONE_OPERATIONS;
grant select on all tables in schema CAPSTONE.CURATED to role CAPSTONE_OPERATIONS;
grant select on all views  in schema CAPSTONE.CURATED to role CAPSTONE_OPERATIONS;
