

-- можно сравнивать
sandbox_analytics.P_GN_ORDER (загружена с 1 сентября 2026)
и
preparation.P_GN_ORDER

CREATE TABLE sandbox_analytics.P_GN_ORDER AS
--EXPLAIN
WITH
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_SALE_ORDER  →  s3_object = SALE_ORDER
-- В Spark: spark.read("SALE_ORDER", keys_for_incremental_load=[...])
--          → temp view ERP_ORDER
-- =====================================================================
erp_order AS (
    SELECT
        eo.sale_number,
        eo.sample_number,
        eo.created_at,
        eo.sale_department,
        eo.sale_doc_kind,
        eo.sale_doc_dype,
        eo.sale_org,
        eo.sale_channel,
        eo.incoterms,
        eo.client_number,
        eo.author,
        eo.transaction_code,
        eo.rejection_reason,
        eo.is_brigadier,
        eo.price_schema,
        eo.telephone_number,
        eo.passanger,
        eo.order_source,
        eo.discount_card,
        eo.project_id,
        eo.cash_payment_amount,
        eo.card_payment_amount,
        eo.promo_action,
        eop.sale_position,
        eop.sample_number         AS sample_number_position,
        eop.sample_position,
        eop.return_type           AS return_type_position,
        eop.created_at            AS created_at_position,
        eop.grade_type,
        eop.plant,
        eop.warehouse,
        eop.product_item,
        eop.substitution_product,
        eop.author                AS author_position,
        eop.net_cost,
        eop.calculate_cost,
        eop.tax,
        eop.rejection_reason      AS rejection_reason_position,
        eop.position_type,
        eop.mark_down_flg,
        eop.gtr_number,
        eop.fact_count,
        eop.fact_count_store,
        eop.guid,
        CASE
            WHEN eo._processed_at > eop._processed_at
            THEN eo._processed_at
            ELSE eop._processed_at
        END                       AS _processed_at,
        CASE
            WHEN eo._deleted IS TRUE OR eop._deleted IS TRUE
            THEN TRUE
            ELSE FALSE
        END                       AS _deleted
    FROM public.ERP_ORDER eo
    LEFT JOIN public.ERP_ORDER_POSITIONS eop
        ON eop.sale_number = eo.sale_number
    WHERE eo.created_at >= '2018-01-01'
    AND CASE
              WHEN eo._processed_at > eop._processed_at THEN eo._processed_at
              ELSE eop._processed_at
          END IS NOT NULL
),
-- =====================================================================
-- Источник: reload_S3_ERP_SITE_LINK_ORDER  →  s3_object = SITE_LINK_ORDER
-- В Spark: spark.read("SITE_LINK_ORDER") → temp view ERP_SITE_LINK_ORDER
-- =====================================================================
erp_site_link_order AS (
    SELECT
        eso.id,
        eso.internet_order_number,
        eso.payment_type,
        esol.erp_sale_number,
        eso.business,
        del.express_pickup
    FROM public.ERP_SITE_ORDER_LINK esol
    LEFT JOIN public.ERP_SITE_ORDER eso
        ON eso.id = esol.id
       AND eso._deleted IS NOT TRUE
    LEFT JOIN public.ERP_SITE_ORDER_DELIVERIES del
        ON del.order_id = eso.id
       AND del._deleted IS NOT TRUE
    WHERE esol._deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_CASHBOOK_PAYMENT  →  CASHBOOK_PAYMENT
-- В Spark: spark.read("CASHBOOK_PAYMENT", keys_for_incremental_load=[...])
--          → temp view ERP_CASHBOOK_PAYMENT
-- =====================================================================
erp_cashbook_payment AS (
    SELECT * FROM public.ERP_PAYMENT_POSITION
    WHERE _processed_at IS NOT NULL
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_ORDER_PAYMENT  →  ORDER_PAYMENT
-- В Spark: spark.read("ORDER_PAYMENT", keys_for_incremental_load=[...])
--          → temp view ERP_ORDER_PAYMENT
-- =====================================================================
erp_order_payment AS (
    SELECT * FROM public.ERP_ORDER_PAYMENT
    WHERE _processed_at IS NOT NULL
    and sale_position = '00'
),
-- =====================================================================
-- Источник: reload_S3_ERP_SALES_ORGANIZATION  →  SALES_ORGANIZATION
-- В Spark: spark.read("SALES_ORGANIZATION") → temp view ERP_SALES_ORGANIZATION
-- =====================================================================
erp_sales_organization AS (
    SELECT * FROM public.ERP_SALES_ORGANIZATION
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: reload_S3_ERP_USER_MOTIVATION  →  USER_MOTIVATION
-- В Spark: spark.read("USER_MOTIVATION") → temp view ERP_USER_MOTIVATION
-- =====================================================================
erp_user_motivation AS (
    SELECT * FROM public.ERP_USER_MOTIVATION
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_PARTNER_POSITION  →  PARTNER_POSITION
-- В Spark: spark.read("PARTNER_POSITION", keys_for_incremental_load=[...])
--          → temp view ERP_PARTNER_POSITION
-- =====================================================================
erp_partner_position AS (
    SELECT * FROM public.ERP_PARTNER_POSITION
    WHERE _processed_at IS NOT NULL
    and partner_role = 'SH'
),
-- =====================================================================
-- Источник: reload_S3_ERP_SALES_DEPARTMENT  →  SALES_DEPARTMENT
-- В Spark читается, но в финальном запросе НЕ используется
-- =====================================================================
erp_sales_department AS (
    SELECT * FROM public.ERP_SALES_DEPARTMENT
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Далее — исходная логика Spark (views и финальный SELECT)
-- =====================================================================
order_header AS (
    SELECT DISTINCT
        created_at AS load_dt,
        created_at AS ord_dt,
        sale_number,
        sale_doc_kind,
        sale_org,
        sale_channel,
        sale_department,
        incoterms,
        client_number,
        author,
        sample_number,
        transaction_code,
        sale_doc_dype,
        rejection_reason,
        is_brigadier,
        price_schema,
        telephone_number,
        passanger,
        project_id,
        cash_payment_amount,
        card_payment_amount,
        promo_action,
        TRIM(LEADING '0' FROM discount_card) AS discount_card,
        order_source
    FROM erp_order
),
delivery_data AS (
    SELECT
        erp_sale_number,
        internet_order_number,
        payment_type,
        CASE
            WHEN express_pickup IS TRUE THEN 'Да'
            ELSE 'Нет'
        END AS express_pickup
    FROM erp_site_link_order
    WHERE business <> 'ENRG'
),
payment AS (
    SELECT
        sale_number,
        cashbox,
        CAST(transaction_number AS INT) AS transaction_number,
        ROW_NUMBER() OVER (
            PARTITION BY sale_number
            ORDER BY payment_number DESC
        ) AS payment_rank
    FROM erp_cashbook_payment
    WHERE sale_number IS NOT NULL
      AND (
            CAST(transaction_number AS INT) IN (30, 31, 4, 5, 100, 101)
            OR cashbox IN ('9851', '9852', '9853', '9854')
          )
),
fnl AS (
	SELECT
	    oh.load_dt,
	    oh.ord_dt,
	    oh.sale_number,
	    oh.sale_doc_kind,
	    oh.sale_org,
	    oh.sale_channel,
	    oh.sale_department,
	    oh.incoterms,
	    oh.client_number,
	    oh.author,
	    oh2.author         AS hdr_samp_author,
	    oh2.sale_doc_kind  AS hdr_samp_doc_kind,
	    oh.sample_number   AS hdr_samp_cd,
	    eum.user_name      AS sales_manager,
	    oh.transaction_code,
	    oh.sale_doc_dype,
	    oh.rejection_reason,
	    oh.is_brigadier,
	    oh.price_schema,
	    oh.telephone_number,
	    oh.passanger,
	    oh.project_id,
	    oh.cash_payment_amount,
	    oh.card_payment_amount,
	    dd.internet_order_number,
	    dd.express_pickup,
	    eop.contract,
	    epp.debitor        AS debitor_sale,
	    oh.promo_action,
	    oh.discount_card,
	    CASE
	        WHEN p.cashbox = '9851' THEN 'Оплата по ссылке'
	        WHEN p.cashbox = '9852' THEN 'По СБП/QR-динамическому'
	        WHEN p.cashbox = '9853' THEN 'Оплата частями'
	        WHEN p.cashbox = '9854' THEN 'СБП B2B'
	        WHEN p.transaction_number IN (4, 5)     THEN 'Оплата наличными'
	        WHEN p.transaction_number IN (30, 31)   THEN 'Оплата по терминалу'
	        WHEN p.transaction_number IN (100, 101) THEN 'Оплата по QR-табличке'
	        WHEN dd.payment_type = 'invoice'        THEN 'Cписанием со счета (ЮЛ)'
	    END AS payment_type,
	    oh.order_source,
	    CASE
	        WHEN eop.shipping_payment_method = 'PT01' THEN TRUE
	        ELSE FALSE
	    END AS local_payment_flag
	FROM order_header oh
	INNER JOIN erp_order_payment eop
	    ON eop.sale_number = oh.sale_number
	   --AND eop.sale_position = '00'
	LEFT JOIN order_header oh2
	    ON oh2.sale_number = oh.sample_number
	LEFT JOIN delivery_data dd
	    ON dd.erp_sale_number = oh.sale_number
	LEFT JOIN erp_sales_organization eso
	    ON eso.sales_organization = oh.sale_org
	LEFT JOIN erp_user_motivation eum
	    ON eum.sale_number = oh.sample_number
	LEFT JOIN erp_partner_position epp
	    ON epp.document_number = oh.sale_number
	  -- AND epp.partner_role = 'SH'
	LEFT JOIN payment p
	    ON p.sale_number = oh.sale_number
	   AND p.payment_rank = 1
	WHERE oh.sale_doc_kind <> 'AG'
	  AND oh.sale_doc_dype IN ('B', 'C', 'H', 'K')
	  AND (eso.business <> 'ENRG' OR eso.business IS NULL)
  )
  SELECT * FROM fnl
WHERE load_dt >= '2026-09-01'::DATE
--ORDER BY oh.sale_number;