-- ============================================================
-- DAY 6: ENVIRONMENT MANAGEMENT (DEV / TEST / PROD)
-- ============================================================
-- Creates TEST and PROD environments as zero-copy clones
-- of the DEV (CAPSTONE) database, then creates environment-
-- specific roles and replicates grants across all environments.
--
-- Pattern: CAPSTONE (DEV) -> CAPSTONE_TEST -> CAPSTONE_PROD
-- Roles:   CAPSTONE_{ENV}_{ADMIN|ANALYST|OPERATIONS}
-- ============================================================


-- ############################################################
-- SECTION 1: CREATE ENVIRONMENT-SPECIFIC ROLES
-- ############################################################

-- DEV roles
create role if not exists CAPSTONE_DEV_ADMIN;
create role if not exists CAPSTONE_DEV_ANALYST;
create role if not exists CAPSTONE_DEV_OPERATIONS;

-- TEST roles
create role if not exists CAPSTONE_TEST_ADMIN;
create role if not exists CAPSTONE_TEST_ANALYST;
create role if not exists CAPSTONE_TEST_OPERATIONS;

-- PROD roles
create role if not exists CAPSTONE_PROD_ADMIN;
create role if not exists CAPSTONE_PROD_ANALYST;
create role if not exists CAPSTONE_PROD_OPERATIONS;

-- All environment roles -> SYSADMIN
grant role CAPSTONE_DEV_ADMIN       to role SYSADMIN;
grant role CAPSTONE_DEV_ANALYST     to role SYSADMIN;
grant role CAPSTONE_DEV_OPERATIONS  to role SYSADMIN;
grant role CAPSTONE_TEST_ADMIN      to role SYSADMIN;
grant role CAPSTONE_TEST_ANALYST    to role SYSADMIN;
grant role CAPSTONE_TEST_OPERATIONS to role SYSADMIN;
grant role CAPSTONE_PROD_ADMIN      to role SYSADMIN;
grant role CAPSTONE_PROD_ANALYST    to role SYSADMIN;
grant role CAPSTONE_PROD_OPERATIONS to role SYSADMIN;


-- ############################################################
-- SECTION 2: CLONE DATABASES
-- ############################################################

create or replace database CAPSTONE_TEST
    clone CAPSTONE
    comment = 'TEST/QA environment - Zero-copy clone of CAPSTONE (DEV)';

create or replace database CAPSTONE_PROD
    clone CAPSTONE
    comment = 'PRODUCTION environment - Zero-copy clone of CAPSTONE (DEV)';


-- ############################################################
-- SECTION 3: DEV_ADMIN GRANTS (full ownership on CAPSTONE)
-- ############################################################

grant usage on warehouse COMPUTE_WH to role CAPSTONE_DEV_ADMIN;

-- Database level
grant usage             on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant monitor           on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant modify            on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant create schema     on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant create database role on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant applybudget       on database CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant execute auto classification on database CAPSTONE to role CAPSTONE_DEV_ADMIN;

-- All schemas in CAPSTONE
grant all privileges on schema CAPSTONE.CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant all privileges on schema CAPSTONE.STG      to role CAPSTONE_DEV_ADMIN;
grant all privileges on schema CAPSTONE.CURATED  to role CAPSTONE_DEV_ADMIN;
grant all privileges on schema CAPSTONE.PUBLIC    to role CAPSTONE_DEV_ADMIN;

-- All tables
grant all privileges on all tables in schema CAPSTONE.CAPSTONE to role CAPSTONE_DEV_ADMIN;
grant all privileges on all tables in schema CAPSTONE.STG      to role CAPSTONE_DEV_ADMIN;
grant all privileges on all tables in schema CAPSTONE.CURATED  to role CAPSTONE_DEV_ADMIN;

-- All views
grant all privileges on all views in schema CAPSTONE.CURATED to role CAPSTONE_DEV_ADMIN;


-- ############################################################
-- SECTION 4: TEST ENVIRONMENT GRANTS
-- ############################################################

-- TEST_ADMIN (same pattern as DEV_ADMIN but on CAPSTONE_TEST)
grant usage on warehouse COMPUTE_WH to role CAPSTONE_TEST_ADMIN;
grant usage on database CAPSTONE_TEST to role CAPSTONE_TEST_ADMIN;
grant all privileges on schema CAPSTONE_TEST.CAPSTONE to role CAPSTONE_TEST_ADMIN;
grant all privileges on schema CAPSTONE_TEST.STG      to role CAPSTONE_TEST_ADMIN;
grant all privileges on schema CAPSTONE_TEST.CURATED  to role CAPSTONE_TEST_ADMIN;
grant all privileges on all tables in schema CAPSTONE_TEST.CAPSTONE to role CAPSTONE_TEST_ADMIN;
grant all privileges on all tables in schema CAPSTONE_TEST.STG      to role CAPSTONE_TEST_ADMIN;
grant all privileges on all tables in schema CAPSTONE_TEST.CURATED  to role CAPSTONE_TEST_ADMIN;
grant all privileges on all views  in schema CAPSTONE_TEST.CURATED  to role CAPSTONE_TEST_ADMIN;

-- TEST_ANALYST (CURATED read-only)
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_TEST_ANALYST;
grant usage on database CAPSTONE_TEST    to role CAPSTONE_TEST_ANALYST;
grant usage on schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_ANALYST;
grant select on all tables in schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_ANALYST;
grant select on all views  in schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_ANALYST;

-- TEST_OPERATIONS (STG + CURATED read-only)
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_TEST_OPERATIONS;
grant usage on database CAPSTONE_TEST    to role CAPSTONE_TEST_OPERATIONS;
grant usage on schema CAPSTONE_TEST.STG     to role CAPSTONE_TEST_OPERATIONS;
grant usage on schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_OPERATIONS;
grant select on all tables in schema CAPSTONE_TEST.STG     to role CAPSTONE_TEST_OPERATIONS;
grant select on all tables in schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_OPERATIONS;
grant select on all views  in schema CAPSTONE_TEST.CURATED to role CAPSTONE_TEST_OPERATIONS;


-- ############################################################
-- SECTION 5: PROD ENVIRONMENT GRANTS
-- ############################################################

-- PROD_ADMIN
grant usage on warehouse COMPUTE_WH to role CAPSTONE_PROD_ADMIN;
grant usage on database CAPSTONE_PROD to role CAPSTONE_PROD_ADMIN;
grant all privileges on schema CAPSTONE_PROD.CAPSTONE to role CAPSTONE_PROD_ADMIN;
grant all privileges on schema CAPSTONE_PROD.STG      to role CAPSTONE_PROD_ADMIN;
grant all privileges on schema CAPSTONE_PROD.CURATED  to role CAPSTONE_PROD_ADMIN;
grant all privileges on all tables in schema CAPSTONE_PROD.CAPSTONE to role CAPSTONE_PROD_ADMIN;
grant all privileges on all tables in schema CAPSTONE_PROD.STG      to role CAPSTONE_PROD_ADMIN;
grant all privileges on all tables in schema CAPSTONE_PROD.CURATED  to role CAPSTONE_PROD_ADMIN;
grant all privileges on all views  in schema CAPSTONE_PROD.CURATED  to role CAPSTONE_PROD_ADMIN;

-- PROD_ANALYST (CURATED read-only)
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_PROD_ANALYST;
grant usage on database CAPSTONE_PROD    to role CAPSTONE_PROD_ANALYST;
grant usage on schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_ANALYST;
grant select on all tables in schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_ANALYST;
grant select on all views  in schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_ANALYST;

-- PROD_OPERATIONS (STG + CURATED read-only)
grant usage on warehouse COMPUTE_WH      to role CAPSTONE_PROD_OPERATIONS;
grant usage on database CAPSTONE_PROD    to role CAPSTONE_PROD_OPERATIONS;
grant usage on schema CAPSTONE_PROD.STG     to role CAPSTONE_PROD_OPERATIONS;
grant usage on schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_OPERATIONS;
grant select on all tables in schema CAPSTONE_PROD.STG     to role CAPSTONE_PROD_OPERATIONS;
grant select on all tables in schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_OPERATIONS;
grant select on all views  in schema CAPSTONE_PROD.CURATED to role CAPSTONE_PROD_OPERATIONS;


-- ############################################################
-- SECTION 6: CROSS-ENVIRONMENT GRANTS FOR BASE ROLES
-- ############################################################
-- The base roles (CAPSTONE_ADMIN/ANALYST/OPERATIONS) also
-- get access to TEST and PROD databases.

-- CAPSTONE_ADMIN -> TEST + PROD
grant usage  on schema CAPSTONE_TEST.CAPSTONE to role CAPSTONE_ADMIN;
grant usage  on schema CAPSTONE_TEST.STG      to role CAPSTONE_ADMIN;
grant usage  on schema CAPSTONE_TEST.CURATED  to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_TEST.CAPSTONE to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_TEST.STG      to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_TEST.CURATED  to role CAPSTONE_ADMIN;
grant select on all views  in schema CAPSTONE_TEST.CURATED  to role CAPSTONE_ADMIN;

grant usage  on schema CAPSTONE_PROD.CAPSTONE to role CAPSTONE_ADMIN;
grant usage  on schema CAPSTONE_PROD.STG      to role CAPSTONE_ADMIN;
grant usage  on schema CAPSTONE_PROD.CURATED  to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_PROD.CAPSTONE to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_PROD.STG      to role CAPSTONE_ADMIN;
grant select on all tables in schema CAPSTONE_PROD.CURATED  to role CAPSTONE_ADMIN;
grant select on all views  in schema CAPSTONE_PROD.CURATED  to role CAPSTONE_ADMIN;

-- CAPSTONE_ANALYST -> TEST + PROD (CURATED only)
grant usage  on schema CAPSTONE_TEST.CURATED to role CAPSTONE_ANALYST;
grant select on all tables in schema CAPSTONE_TEST.CURATED to role CAPSTONE_ANALYST;
grant select on all views  in schema CAPSTONE_TEST.CURATED to role CAPSTONE_ANALYST;

grant usage  on schema CAPSTONE_PROD.CURATED to role CAPSTONE_ANALYST;
grant select on all tables in schema CAPSTONE_PROD.CURATED to role CAPSTONE_ANALYST;
grant select on all views  in schema CAPSTONE_PROD.CURATED to role CAPSTONE_ANALYST;

-- CAPSTONE_OPERATIONS -> TEST + PROD (STG + CURATED)
grant usage  on schema CAPSTONE_TEST.STG     to role CAPSTONE_OPERATIONS;
grant usage  on schema CAPSTONE_TEST.CURATED to role CAPSTONE_OPERATIONS;
grant select on all tables in schema CAPSTONE_TEST.STG     to role CAPSTONE_OPERATIONS;
grant select on all tables in schema CAPSTONE_TEST.CURATED to role CAPSTONE_OPERATIONS;
grant select on all views  in schema CAPSTONE_TEST.CURATED to role CAPSTONE_OPERATIONS;

grant usage  on schema CAPSTONE_PROD.STG     to role CAPSTONE_OPERATIONS;
grant usage  on schema CAPSTONE_PROD.CURATED to role CAPSTONE_OPERATIONS;
grant select on all tables in schema CAPSTONE_PROD.STG     to role CAPSTONE_OPERATIONS;
grant select on all tables in schema CAPSTONE_PROD.CURATED to role CAPSTONE_OPERATIONS;
grant select on all views  in schema CAPSTONE_PROD.CURATED to role CAPSTONE_OPERATIONS;
