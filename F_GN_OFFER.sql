WITH
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_SALE_ORDER → s3_object = "SALE_ORDER"
-- Spark: spark.read_with_filter("SALE_ORDER", column_for_filter="created_at",
--                               keys_for_incremental_load=['sale_number','sale_position'])
-- DAG: ERP_ORDER ⨝ ERP_ORDER_POSITIONS, created_at >= '2018-01-01'
-- =====================================================================


-- можно сравнивать
sandbox_analytics.F_GN_OFFER (загружена с 1 сентября 2026)
и
prepared.F_GN_OFFER

CREATE TABLE sandbox_analytics.F_GN_OFFER AS
--EXPLAIN
WITH
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
        eop.sample_number    AS sample_number_position,
        eop.sample_position,
        eop.return_type      AS return_type_position,
        eop.created_at       AS created_at_position,
        eop.grade_type,
        eop.plant,
        eop.warehouse,
        eop.product_item,
        eop.substitution_product,
        eop.author           AS author_position,
        eop.net_cost,
        eop.calculate_cost,
        eop.tax,
        eop.rejection_reason AS rejection_reason_position,
        eop.position_type,
        eop.mark_down_flg,
        eop.gtr_number,
        eop.fact_count,
        eop.fact_count_store,
        eop.guid
    FROM public.ERP_ORDER eo
    LEFT JOIN public.ERP_ORDER_POSITIONS eop
        ON eop.sale_number = eo.sale_number
    WHERE eo.created_at >= '2018-01-01'
		AND eo.created_at >= TIMESTAMP '2026-09-01 00:00:00'   -- новая строка
    AND CASE
              WHEN eo._processed_at > eop._processed_at THEN eo._processed_at
              ELSE eop._processed_at
          END IS NOT NULL
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_ORDER_PAYMENT → s3_object = "ORDER_PAYMENT"
-- DAG: SELECT * FROM public.ERP_ORDER_PAYMENT
-- =====================================================================
erp_order_payment AS (
    SELECT * FROM public.ERP_ORDER_PAYMENT
    WHERE _processed_at IS NOT NULL
),
-- =====================================================================
-- Источник: reload_S3_ERP_SALES_ORGANIZATION → s3_object = "SALES_ORGANIZATION"
-- DAG: SELECT * FROM public.ERP_SALES_ORGANIZATION WHERE _deleted IS NOT TRUE
-- =====================================================================
erp_sales_organization AS (
    SELECT * FROM public.ERP_SALES_ORGANIZATION
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_PARTNER_POSITION → s3_object = "PARTNER_POSITION"
-- DAG: SELECT * FROM public.ERP_PARTNER_POSITION
-- =====================================================================
erp_partner_position AS (
    SELECT * FROM public.ERP_PARTNER_POSITION
    WHERE _processed_at IS NOT NULL
),
-- =====================================================================
-- Источник: reload_S3_ERP_USER_MOTIVATION → s3_object = "USER_MOTIVATION"
-- DAG: SELECT * FROM public.ERP_USER_MOTIVATION WHERE _deleted IS NOT TRUE
-- =====================================================================
erp_user_motivation AS (
    SELECT * FROM public.ERP_USER_MOTIVATION
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: init_inc_load_S3_ERP_INVOICE → s3_object = "INVOICE"
-- DAG: ERP_INVOICE ⨝ ERP_INVOICE_POSITIONS
-- =====================================================================
erp_invoice AS (
    SELECT
        ei.invoice_number,
        ei.invoice_date,
        ei.sales_org,
        ei.sales_channel,
        ei.price_condition_number,
        ei.sales_doc_type,
        ei.company_code,
        ei.invoice_kind,
        ei.created_by,
        ei.payer LIKE 'T%'     AS payer_startwith_t_flg,
        ei.payer,
        ei.client_number,
        ei.incoterms,
        ei.reversal_document_number,
        ei.price_list,
        ei.invoice_reference_number,
        ei.created_at,
        eip.position_number,
        eip.trade_doc_number,
        eip.trade_doc_pos,
        eip.sales_department,
        eip.mvp,
        eip.sample_doc_number,
        eip.sample_doc_pos,
        eip.product_item,
        eip.plant,
        eip.return_type,
        eip.net_cost,
        eip.nds_proceeds,
        eip.grade_type,
        eip.sale_uom,
        eip.base_uom,
        eip.position_type,
        eip.shipment_point,
        eip.type_of_payment,
        eip.condition_subtotal,
        eip.batch_number,
        eip.warehouse,
        eip.calculate_cost,
        eip.fact_count,
        eip.fact_count_store,
        eip.sale_price,
        eip.sale_price_with_disc,
        CASE
            WHEN ei._processed_at > eip._processed_at THEN ei._processed_at
            ELSE eip._processed_at
        END AS _processed_at,
        CASE
            WHEN ei._deleted IS TRUE OR eip._deleted IS TRUE THEN TRUE
            ELSE FALSE
        END AS _deleted
    FROM public.ERP_INVOICE ei
    LEFT JOIN public.ERP_INVOICE_POSITIONS eip
        ON ei.invoice_number = eip.invoice_number
    WHERE CASE
              WHEN ei._processed_at > eip._processed_at THEN ei._processed_at
              ELSE eip._processed_at
          END IS NOT NULL
),
-- =====================================================================
-- Источник: reload_S3_ERP_SITE_LINK_ORDER → s3_object = "SITE_LINK_ORDER"
-- DAG: ERP_SITE_ORDER_LINK ⨝ ERP_SITE_ORDER ⨝ ERP_SITE_ORDER_DELIVERIES
-- =====================================================================
erp_site_link_order AS (
    SELECT
        eso.id,
        esol.erp_sale_number
    FROM public.ERP_SITE_ORDER_LINK esol
    LEFT JOIN public.ERP_SITE_ORDER eso
        ON eso.id = esol.id
       AND eso._deleted IS NOT TRUE
    WHERE esol._deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: reload_S3_ERP_SALES_DEPARTMENT → s3_object = "SALES_DEPARTMENT"
-- =====================================================================
erp_sales_department AS (
    SELECT * FROM public.ERP_SALES_DEPARTMENT
    WHERE _deleted IS NOT TRUE
),
-- =====================================================================
-- Источник: reload_S3_P_GN_ORD_SALES_DEPT_SUB → s3_object = "P_GN_ORD_SALES_DEPT_SUB"
-- DAG: SELECT * FROM preparation.P_GN_ORD_SALES_DEPT_SUB
-- =====================================================================
p_gn_ord_sales_dept_sub AS (
    SELECT * FROM preparation.P_GN_ORD_SALES_DEPT_SUB
),
-- =====================================================================
-- I_GN_DEBITOR_EXCEPTIONS — подготовка (подтверждено)
-- =====================================================================
i_gn_debitor_exceptions AS (
    SELECT gn_debitor_cd FROM preparation.I_GN_DEBITOR_EXCEPTIONS
),
-- =====================================================================
-- Spark view FILTERED_INVOICE
-- =====================================================================
filtered_invoice AS (
    SELECT *
    FROM erp_invoice
    WHERE client_number NOT IN (SELECT gn_debitor_cd FROM i_gn_debitor_exceptions)
),
-- =====================================================================
-- Spark view OFFERS
-- Отличия от FGnOrder:
--   * sale_doc_kind = 'AG'  (а не <> 'AG')
--   * join ERP_ORDER_PAYMENT по sale_position = sale_position (а не '00')
--   * incoterms: COALESCE-логика
--   * нет hdr_samp_author / hdr_samp_cd / hdr_samp_doc_kind
--   * фильтр eso.business <> 'ENRG' без OR IS NULL
-- =====================================================================
offers AS (
    SELECT
        eo.created_at                AS ordr_dt,
        eo.created_at_position       AS ordr_pos_dt,
        eo.sale_number,
        eo.sale_position,
        eo.grade_type,
        eo.plant,
        COALESCE(eo.warehouse, '')   AS warehouse,
        eo.sale_doc_kind,
        eo.sale_org,
        eo.sale_channel,
        eo.sale_department,
        CASE
            WHEN eop.incoterms IS NULL THEN eo.incoterms
            ELSE eop.incoterms
        END AS incoterms,
        eo.client_number,
        epp.debitor                  AS debitor_sale,
        eo.product_item,
        eo.author                    AS ordr_ahr,
        eo.author_position           AS ordr_ahr_hdr,
        eum.user_name                AS sales_manager,
        eo.transaction_code,
        eo.sale_doc_dype,
        eo.net_cost,
        eo.calculate_cost,
        eo.tax,
        eo.rejection_reason          AS ordr_rjct_rsn,
        eo.rejection_reason_position AS ordr_pos_rjct_rsn,
        eo.return_type_position      AS return_type,
        CASE
            WHEN (eo.sale_doc_dype IN ('H','K','N','O','T','6') AND eo.return_type_position IS NOT TRUE)
              OR (eo.sale_doc_dype NOT IN ('H','K','N','O','T','6') AND eo.return_type_position IS TRUE)
            THEN -1
            ELSE 1
        END AS change_sign_flag,
        CASE
            WHEN eo.return_type_position IS TRUE THEN -eo.fact_count
            ELSE eo.fact_count
        END AS fact_count,
        CASE
            WHEN eo.return_type_position IS TRUE THEN -eo.fact_count_store
            ELSE eo.fact_count_store
        END AS fact_count_store,
        eo.sample_number             AS sample_number
    FROM erp_order eo
    LEFT JOIN erp_order_payment eop
        ON eop.sale_number   = eo.sale_number
       AND eop.sale_position = eo.sale_position
    LEFT JOIN erp_sales_organization eso
        ON eso.sales_organization = eo.sale_org
    LEFT JOIN erp_partner_position epp
        ON epp.document_number = eo.sale_number
       AND epp.partner_role    = 'SH'
    LEFT JOIN erp_user_motivation eum
        ON eum.sale_number = eo.sample_number
    WHERE eo.sale_position IS NOT NULL
      AND eo.sale_doc_kind = 'AG'
      AND eo.sale_doc_dype IN ('B','C','H','K')
      AND (
            eo.plant NOT IN ('1918','1919','1928','1929','1938','1939','1948','1949','3999')
         OR eo.plant IS NULL
      )
      AND eso.business <> 'ENRG'
),
-- =====================================================================
-- Spark view SITE_ORDER_LINK_PREP
-- =====================================================================
site_order_link_prep AS (
    SELECT id, erp_sale_number
    FROM erp_site_link_order
    GROUP BY id, erp_sale_number
),
-- =====================================================================
-- Spark view CHANGE_SALE_DEP_RETURN
-- Отличия от FGnOrder: используется o.sale_department NOT IN ('1099','1098'),
-- BOOL_OR(...) заменён на MAX(...) OVER (PARTITION BY ...)  — эквивалентно.
-- ВНИМАНИЕ: join esol1 ON esol1.erp_sale_number = o.sale_position
-- (в FGnOrder было по o.sample_number). Оставлено как в источнике.
-- =====================================================================
change_sale_dep_return AS (
    SELECT
        CASE
            WHEN o.sale_department NOT IN ('1099','1098')
            THEN COALESCE(i_p.sales_department, o.sale_department)
            ELSE
                CASE
                    WHEN MAX(
                            CASE
                                WHEN COALESCE(esol.id, esol1.id, esol2.id, sub.site_id) IS NULL
                                THEN FALSE
                                ELSE TRUE
                            END
                         ) OVER (PARTITION BY o.sale_number)
                    THEN '1098'
                    ELSE '1099'
                END
        END AS sales_department_imkc,
        o.*
    FROM offers o
    LEFT JOIN filtered_invoice i_p
        ON i_p.invoice_number  = o.sale_number
       AND i_p.position_number = o.sale_position
    LEFT JOIN p_gn_ord_sales_dept_sub sub
        ON sub.sale_number   = o.sale_number
       AND sub.sale_position = o.sale_position
    LEFT JOIN site_order_link_prep esol
        ON esol.erp_sale_number  = o.sale_number
    LEFT JOIN site_order_link_prep esol1
        ON esol1.erp_sale_number = o.sale_position
    LEFT JOIN site_order_link_prep esol2
        ON esol2.erp_sale_number = i_p.trade_doc_number
),
-- =====================================================================
-- Spark view OFFERS_WITH_NEED_FORMATS
-- =====================================================================
offers_with_need_formats AS (
    SELECT
        s.*,
        esd.business_format  AS bsn_frmt,
        esd2.business_format AS bsn_frmt_imkc
    FROM change_sale_dep_return s
    LEFT JOIN erp_sales_department esd
        ON esd.sales_department = s.sale_department
    LEFT JOIN erp_sales_department esd2
        ON esd2.sales_department = s.sales_department_imkc
),
-- =====================================================================
-- Spark view PREP_OFFERS
-- =====================================================================
prep_offers AS (
    SELECT
        ordr_dt,
        ordr_pos_dt,
        sale_number,
        sale_position,
        grade_type,
        plant,
        warehouse,
        sale_doc_kind,
        sale_org,
        sale_channel,
        sale_department,
        sales_department_imkc,
        incoterms,
        client_number,
        debitor_sale,
        product_item,
        ordr_ahr,
        ordr_ahr_hdr,
        sales_manager,
        transaction_code,
        sale_doc_dype,
        ordr_rjct_rsn,
        ordr_pos_rjct_rsn,
        bsn_frmt,
        bsn_frmt_imkc,
        net_cost,
        calculate_cost,
        tax,
        fact_count,
        fact_count_store,
        CASE
            WHEN return_type IS TRUE THEN -change_sign_flag
            ELSE change_sign_flag
        END AS change_sign_flag,
        CASE WHEN bsn_frmt_imkc = 'SDDIS' THEN TRUE ELSE FALSE END AS tv_flag,
        CASE
            WHEN bsn_frmt_imkc IN ('PHBL','PHVIN','VINS') THEN TRUE
            ELSE FALSE
        END AS ph_flg,
        CASE
            WHEN bsn_frmt_imkc IN ('NEWP','SDBIG','SDBRG','SDKOT','SDMOK',
                                   'IMKC','IM','KC','SDKR','SDSP')
              OR (bsn_frmt_imkc = 'TEPLO' AND sale_channel IN ('02','03','04'))
            THEN TRUE
            ELSE FALSE
        END AS sd_flag
    FROM offers_with_need_formats
)
-- =====================================================================
-- Финальный SELECT — то, что Spark пишет в prepared.F_GN_OFFER
-- =====================================================================
SELECT
    po.ordr_dt AS load_dt,
    COALESCE(d1.gn_grade_type_id, -1)          AS gn_grade_type_id,
    COALESCE(d2.gn_plant_id, -1)               AS gn_plant_id,
    COALESCE(d3.gn_shipment_place_id, -1)      AS gn_shipment_place_id,
    COALESCE(d4.gn_sale_document_kind_id, -1)  AS gn_sale_document_kind_id,
    COALESCE(d5.gn_sales_organization_id, -1)  AS gn_sales_organization_id,
    COALESCE(d6.gn_sale_channel_id, -1)        AS gn_sale_channel_id,
    COALESCE(d7.gn_sales_department_id, -1)    AS gn_sales_department_id1,
    COALESCE(d8.gn_sales_department_id, -1)    AS gn_sales_department_id2,
    COALESCE(d9.gn_incoterms_id, -1)           AS gn_incoterms_id,
    COALESCE(d10.gn_debitor_id, -1)            AS gn_debitor_id1,
    COALESCE(d11.gn_debitor_id, -1)            AS gn_debitor_id2,
    COALESCE(d12.gn_product_id, -1)            AS gn_product_id,
    COALESCE(d13.gn_employee_id, -1)           AS gn_employee_id1,
    COALESCE(d14.gn_employee_id, -1)           AS gn_employee_id2,
    COALESCE(d15.gn_employee_id, -1)           AS gn_employee_id3,
    COALESCE(d16.gn_transaction_sap_id, -1)    AS gn_transaction_sap_id,
    COALESCE(d17.gn_sale_document_type_id, -1) AS gn_sale_document_type_id,
    COALESCE(d18.gn_order_reason_id, -1)       AS gn_order_reason_id,
    COALESCE(d19.gn_rejection_reason_id, -1)   AS gn_rejection_reason_id,
    COALESCE(d20.gn_business_format_id, -1)    AS gn_business_format_id1,
    COALESCE(d21.gn_business_format_id, -1)    AS gn_business_format_id2,
    po.ordr_dt                                 AS ofr_dt,
    po.ordr_pos_dt                             AS ofr_pos_dt,
    po.sale_number                             AS ofr_cd,
    po.sale_position                           AS ofr_pos_nbr,
    (po.net_cost       * po.change_sign_flag)  AS net_cost,
    (po.calculate_cost * po.change_sign_flag)  AS clc_cost,
    (po.tax            * po.change_sign_flag)  AS tax,
    po.fact_count                              AS prch_cnt,
    po.fact_count_store                        AS base_cnt,
    po.tv_flag,
    po.ph_flg                                  AS ph_flag,
    po.sd_flag
FROM prep_offers po
LEFT JOIN prepared.D_GN_GRADE_TYPE d1
    ON d1.gn_grade_type_cd = po.grade_type
LEFT JOIN prepared.D_GN_PLANT d2
    ON d2.gn_plant_cd = po.plant
LEFT JOIN prepared.D_GN_SHIPMENT_PLACE d3
    ON d3.gn_shipment_place_cd = po.warehouse || po.plant
LEFT JOIN prepared.D_GN_SALE_DOCUMENT_KIND d4
    ON d4.gn_sale_document_kind_cd = po.sale_doc_kind
LEFT JOIN prepared.D_GN_SALES_ORGANIZATION d5
    ON d5.gn_sales_organization_cd = po.sale_org
LEFT JOIN prepared.D_GN_SALE_CHANNEL d6
    ON d6.gn_sale_channel_cd = po.sale_channel
LEFT JOIN prepared.D_GN_SALES_DEPARTMENT d7
    ON d7.gn_sales_department_cd = po.sale_department
LEFT JOIN prepared.D_GN_SALES_DEPARTMENT d8
    ON d8.gn_sales_department_cd = po.sales_department_imkc
LEFT JOIN prepared.D_GN_INCOTERMS d9
    ON d9.gn_incoterms_cd = po.incoterms
LEFT JOIN prepared.D_GN_DEBITOR d10
    ON d10.gn_debitor_cd = po.client_number
LEFT JOIN prepared.D_GN_DEBITOR d11
    ON d11.gn_debitor_cd = po.debitor_sale
LEFT JOIN prepared.D_GN_PRODUCT d12
    ON d12.gn_product_cd = po.product_item
LEFT JOIN prepared.D_GN_EMPLOYEE d13
    ON d13.gn_employee_cd = po.ordr_ahr
LEFT JOIN prepared.D_GN_EMPLOYEE d14
    ON d14.gn_employee_cd = po.ordr_ahr_hdr
LEFT JOIN prepared.D_GN_EMPLOYEE d15
    ON d15.gn_employee_cd = po.sales_manager
LEFT JOIN prepared.D_GN_TRANSACTION_SAP d16
    ON d16.gn_transaction_sap_cd = po.transaction_code
LEFT JOIN prepared.D_GN_SALE_DOCUMENT_TYPE d17
    ON d17.gn_sale_document_type_cd = po.sale_doc_dype
LEFT JOIN prepared.D_GN_ORDER_REASON d18
    ON d18.gn_order_reason_cd = po.ordr_rjct_rsn
LEFT JOIN prepared.D_GN_REJECTION_REASON d19
    ON d19.gn_rejection_reason_cd = po.ordr_pos_rjct_rsn
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT d20
    ON d20.gn_business_format_cd = po.bsn_frmt
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT d21
    ON d21.gn_business_format_cd = po.bsn_frmt_imkc
WHERE load_dt >= '2026-09-01'::DATE