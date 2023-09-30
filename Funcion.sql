CREATE OR REPLACE FUNCTION `practica-sql-bigquery.keepcoding.clean_interger`(x INT64) RETURNS INT64 AS (
ifnull(x, -999999));