-- Creación de la tabla ivr_summary
  CREATE OR REPLACE TABLE keepcoding.ivr_summary AS
  WITH 
  other 
    AS (SELECT 
      detail.calls_ivr_id AS ivr_id
    , detail.calls_phone_number AS phone_number
    , detail.calls_ivr_result AS ivr_result
    , detail.calls_start_date AS start_date
    , detail.calls_end_date AS end_date
    , detail.calls_total_duration AS total_duration
    , detail.calls_customer_segment AS customer_segment
    , detail.calls_ivr_language AS ivr_language
    , detail.calls_steps_module AS steps_module
    , detail.calls_module_aggregation AS module_aggregation
    FROM keepcoding.ivr_detail detail
    GROUP BY ivr_id, phone_number, ivr_result, start_date, end_date, total_duration, customer_segment, ivr_language, steps_module, module_aggregation
    ORDER BY detail.calls_ivr_id )

  , label
    AS (SELECT detail.calls_ivr_id
       , CASE WHEN STARTS_WITH(detail.calls_vdn_label, "ATC") THEN "FRONT"
              WHEN STARTS_WITH(detail.calls_vdn_label, "TECH") THEN "TECH"
              WHEN detail.calls_vdn_label = "ABSORPTION" THEN "ABSORPTION"
        ELSE "RESTO"
        END AS vdn_aggregation
    FROM keepcoding.ivr_detail detail)

    , identification
    AS (SELECT detail.calls_ivr_id 
        , COALESCE(NULLIF(detail.document_type, "NULL"), NULLIF(steps.document_type, "NULL")) as document_type
        , COALESCE(NULLIF(detail.document_identification, "NULL"), NULLIF(steps.document_identification, "NULL")) as document_identification
    FROM keepcoding.ivr_detail detail
    JOIN keepcoding.ivr_steps steps ON steps.ivr_id = detail.calls_ivr_id
    GROUP BY detail.calls_ivr_id, document_type, document_identification
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(detail.calls_ivr_id AS STRING)
    ORDER BY detail.calls_ivr_id,document_type DESC, document_identification DESC) = 1)

    , customer
     AS (SELECT detail.calls_ivr_id
        , COALESCE(NULLIF(detail.customer_phone, "NULL"), NULLIF(steps.customer_phone, "NULL")) AS customer_phone
        , COALESCE(NULLIF(detail.billing_account_id, "NULL"), NULLIF(steps.billing_account_id, "NULL")) AS billing_account_id
    FROM keepcoding.ivr_detail detail
    JOIN keepcoding.ivr_steps steps ON steps.ivr_id = detail.calls_ivr_id
    GROUP BY detail.calls_ivr_id, customer_phone, billing_account_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY CAST(detail.calls_ivr_id AS STRING)
    ORDER BY detail.calls_ivr_id,customer_phone DESC, billing_account_id DESC) = 1)

    
    , module
    AS (SELECT detail.calls_ivr_id 
        ,IF(CONTAINS_SUBSTR(detail.calls_module_aggregation, "AVERIA_MASIVA"), 1, 0) as masiva_lg
    FROM keepcoding.ivr_detail detail
    GROUP BY calls_ivr_id, masiva_lg)
 
    , step
    AS (SELECT detail.calls_ivr_id
        , IF(detail.step_name = "CUSTOMERINFOBYPHONE.TX" AND detail.step_description_error is NULL, 1, 0) AS info_by_phone_lg
        , IF(detail.step_name = "CUSTOMERINFOBYDNI.TX" AND detail.step_description_error is NULL, 1, 0) AS info_by_dni_lg
    FROM keepcoding.ivr_detail detail
    GROUP BY detail.calls_ivr_id, info_by_phone_lg, info_by_dni_lg
    ORDER BY detail.calls_ivr_id)

    , calls
    AS (SELECT detail.calls_ivr_id
        , LAG(detail.calls_start_date) OVER (PARTITION BY detail.calls_phone_number ORDER BY detail.calls_start_date) AS previous_call
        , LEAD(detail.calls_start_date) OVER (PARTITION BY detail.calls_phone_number ORDER BY detail.calls_start_date) AS next_call
    FROM keepcoding.ivr_detail detail)
      
  SELECT 
      other.ivr_id AS ivr_id
    , other.phone_number AS phone_number
    , other.ivr_result AS ivr_result
    , label.vdn_aggregation AS vdn_aggregation
    , other.start_date AS start_date
    , other.end_date AS end_date
    , other.total_duration AS total_duration
    , other.customer_segment AS customer_segment
    , other.ivr_language AS ivr_language
    , other.steps_module AS steps_module
    , other.module_aggregation AS module_aggregation
    , identification.document_type AS document_type
    , identification.document_identification AS document_identification
    , customer.customer_phone AS customer_phone
    , customer.billing_account_id AS billing_account_id 
    , module.masiva_lg AS masiva_lg
    , step.info_by_phone_lg AS info_by_phone_lg
    , step.info_by_dni_lg AS info_by_dni_lg
    , IF(DATETIME_DIFF(detail.calls_start_date, calls.previous_call,HOUR)<24,1,0) AS repeated_phone_24H
    , IF(DATETIME_DIFF(calls.next_call,detail.calls_end_date,HOUR)<24,1,0) AS cause_recall_phone_24H

  FROM keepcoding.ivr_detail detail 
  LEFT JOIN other ON detail.calls_ivr_id = other.ivr_id
  LEFT JOIN label ON detail.calls_ivr_id = label.calls_ivr_id
  LEFT JOIN identification ON detail.calls_ivr_id = identification.calls_ivr_id
  LEFT JOIN customer ON detail.calls_ivr_id = customer.calls_ivr_id
  LEFT JOIN step ON detail.calls_ivr_id = step.calls_ivr_id
  LEFT JOIN module ON detail.calls_ivr_id = module.calls_ivr_id
  LEFT JOIN calls ON detail.calls_ivr_id = calls.calls_ivr_id
  
  GROUP BY
    ivr_id
  , phone_number
  , ivr_result
  , vdn_aggregation
  , start_date
  , end_date
  , total_duration
  , customer_segment
  , ivr_language
  , steps_module
  , module_aggregation
  , document_type 
  , document_identification 
  , customer_phone 
  , billing_account_id 
  , masiva_lg 
  , info_by_phone_lg 
  , info_by_dni_lg 
  , repeated_phone_24H
  , cause_recall_phone_24H
ORDER BY phone_number, ivr_id;