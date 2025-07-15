# Week 7 - Daily Data Ingestion Pipeline

This project handles ingestion of three types of files into SQL tables using a truncate-load approach on a daily basis.

## File Types and Logic

1. **CUST_MSTR_YYYYMMDD.csv**
   - Extracts date from filename (e.g., 2019-11-12)
   - Adds column: `Date`
   - Loads into table: `CUST_MSTR`

2. **master_child_export-YYYYMMDD.csv**
   - Extracts date and date key
   - Adds columns: `Date`, `DateKey`
   - Loads into table: `master_child`

3. **H_ECOM_ORDER.csv**
   - Directly loads data (no transformations)
   - Loads into table: `H_ECOM_Orders`

## Run Mode
- Truncate and reload all tables daily.
