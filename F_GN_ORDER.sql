CREATE TABLE sandbox_analytics.F_GN_ORDER (
    load_dt DATE,
    gn_grade_type_id INT,
    gn_plant_id INT,
    gn_shipment_place_id INT,
    gn_sale_document_kind_id INT,
    gn_sales_organization_id INT,
    gn_sales_organization_id1 INT,
    gn_sales_organization_id2 INT,
    gn_sale_channel_id INT,
    gn_sales_department_id1 INT,
    gn_sales_department_id2 INT,
    gn_incoterms_id INT,
    gn_debitor_id1 INT,
    gn_debitor_id2 INT,
    gn_product_id INT,
    gn_product_id2 INT,
    gn_employee_id1 INT,
    gn_employee_id2 INT,
    gn_employee_id3 INT,
    gn_employee_id4 INT,
    gn_transaction_sap_id INT,
    gn_sale_document_type_id INT,
    gn_order_reason_id INT,
    gn_rejection_reason_id INT,
    gn_business_format_id1 INT,
    gn_business_format_id2 INT,
    gn_price_schema_id INT,
    gn_position_type_id INT,
    gn_object_data_id INT,
    gn_employee_id5 INT,
    gn_employee_id6 INT,
    ordr_dt TIMESTAMPTZ,
    ordr_pos_dt TIMESTAMPTZ,
    ordr_cd VARCHAR(10),
    ordr_pos_nbr VARCHAR(6),
    hdr_samp_cd VARCHAR(10),
    samp_cd VARCHAR(10),
    samp_pos_nbr VARCHAR(6),
    mp_ordr_nbr VARCHAR(140),
    net_cost NUMERIC(15, 2),
    clc_cost NUMERIC(15, 2),
    tax NUMERIC(15, 2),
    prch_cnt NUMERIC(15, 3),
    base_cnt NUMERIC(15, 3),
    brgdr_f BOOLEAN,
    tel_nbr VARCHAR(50),
    site_ordr_cd VARCHAR(60),
    exprs_eqmt VARCHAR(6),
    cntrct VARCHAR(140),
    pmnt_tp VARCHAR(58),
    mrkd_f BOOLEAN,
    dcn_card_nbr VARCHAR(20),
    gtr_nbr VARCHAR(15),
    init_ordr_nbr VARCHAR(10),
    init_ordr_pos_nbr VARCHAR(6),
    promo_actn VARCHAR(8),
    ordr_src_nm VARCHAR(40),
    lcl_pmnt_flg BOOLEAN,
    tv_flg BOOLEAN,
    sd_flg BOOLEAN,
    sk_flg BOOLEAN,
    ob_flg BOOLEAN,
    ph_flg BOOLEAN
);

INSERT INTO sandbox_analytics.F_GN_ORDER
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
        eop.sample_number   AS sample_number_position,
        eop.sample_position,
        eop.return_type     AS return_type_position,
        eop.created_at      AS created_at_position,
        eop.grade_type,
        eop.plant,
        eop.warehouse,
        eop.product_item,
        eop.substitution_product,
        eop.author          AS author_position,
        eop.net_cost,
        eop.calculate_cost,
        eop.tax,
        eop.rejection_reason AS rejection_reason_position,
        eop.position_type,
        eop.mark_down_flg,
        eop.gtr_number,
        eop.fact_count,
        eop.fact_count_store,
        eop.guid,
        CASE
            WHEN eo._processed_at > eop._processed_at THEN eo._processed_at
            ELSE eop._processed_at
        END AS _processed_at,
        CASE
            WHEN eo._deleted IS TRUE OR eop._deleted IS TRUE THEN TRUE
            ELSE FALSE
        END AS _deleted
    FROM public.ERP_ORDER eo
    LEFT JOIN public.ERP_ORDER_POSITIONS eop
        ON eop.sale_number = eo.sale_number
    WHERE eo.created_at >= DATE '2018-01-01'
      AND eo._deleted IS NOT TRUE
      AND eop._deleted IS NOT TRUE
),
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
    WHERE ei._deleted IS NOT TRUE
      AND eip._deleted IS NOT TRUE
    ),
erp_sales_department AS (
    SELECT * FROM public.ERP_SALES_DEPARTMENT
    WHERE _deleted IS NOT TRUE
),
erp_order_payment AS (
    SELECT * FROM public.ERP_ORDER_PAYMENT
    WHERE _deleted IS NOT TRUE
),
order_header_author AS (
    SELECT sale_number, author
    FROM public.ERP_ORDER
),
i_gn_debitor_exceptions AS (
    SELECT gn_debitor_cd FROM preparation.I_GN_DEBITOR_EXCEPTIONS
),
filtered_invoice AS (
    SELECT *
    FROM erp_invoice
    WHERE client_number NOT IN (SELECT gn_debitor_cd FROM i_gn_debitor_exceptions)
),
orders AS (
    SELECT
        pgo.ord_dt          AS ordr_dt,
        pgo.sale_doc_kind,
        pgo.sale_org,
        pgo.sale_channel,
        pgo.sale_department,
        pgo.incoterms,
        pgo.client_number,
        pgo.author          AS ordr_ahr,
        pgo.hdr_samp_author AS hdr_samp_author,
        pgo.sales_manager,
        pgo.transaction_code,
        pgo.sale_doc_dype,
        pgo.rejection_reason AS ordr_rjct_rsn,
        pgo.is_brigadier,
        pgo.price_schema,
        pgo.telephone_number,
        pgo.passanger,
        pgo.project_id,
        pgo.cash_payment_amount,
        pgo.card_payment_amount,
        pgo.internet_order_number,
        pgo.express_pickup,
        pgo.contract,
        pgo.debitor_sale,
        pgo.payment_type,
        pgo.discount_card,
        pgo.promo_action,
        pgo.hdr_samp_doc_kind,
        eo.created_at_position AS ordr_pos_dt,
        eo.sale_number,
        eo.sale_position,
        pgo.hdr_samp_cd,
        eo.sample_number_position AS sample_number,
        eo.sample_position,
        eo.grade_type,
        eo.plant,
        COALESCE(eo.warehouse, '') AS warehouse,
        eo.product_item,
        eo.substitution_product,
        eo.author_position AS ordr_ahr_hdr,
        eo.net_cost,
        eo.calculate_cost,
        eo.tax,
        eo.rejection_reason_position AS ordr_pos_rjct_rsn,
        eo.return_type_position      AS return_type,
        eo.position_type,
        eo.mark_down_flg,
        eo.gtr_number,
        eo.sale_doc_kind             AS ordr_doc_kind,
        CASE
            WHEN (pgo.sale_doc_dype IN ('H','K','N','O','T','6') AND eo.return_type_position IS NOT TRUE)
              OR (pgo.sale_doc_dype NOT IN ('H','K','N','O','T','6') AND eo.return_type_position IS TRUE)
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
        pgo.order_source,
        pgo.local_payment_flag
    FROM preparation.P_GN_ORDER pgo
    INNER JOIN erp_order eo
        ON eo.sale_number = pgo.sale_number
    WHERE
      eo.sale_position IS NOT NULL
      AND (
            eo.plant NOT IN (
                '1918','1919','1928','1929','1938','1939','1948','1949','3999'
            )
         OR eo.plant IS NULL
      )
),
site_order_link_prep AS (
    SELECT id, erp_sale_number
    FROM erp_site_link_order
    GROUP BY id, erp_sale_number
),
change_sale_dep_return_prep AS (
    SELECT
        CASE
            WHEN REGEXP_LIKE(o.client_number, '^[A-Z]') IS NOT TRUE
            THEN
                CASE
                    WHEN o.sale_department = '1021' AND o.sale_channel = '01'
                    THEN '1020'
                    ELSE
                        CASE
                            WHEN i.sales_department IS NOT NULL
                            THEN
                                CASE
                                    WHEN i.sales_department = '1021' AND i.sales_channel = '01'
                                    THEN '1020'
                                    ELSE i.sales_department
                                END
                            ELSE o.sale_department
                        END
                END
            ELSE o.sale_department
        END AS sales_department_imkc_prep,
        CASE
            WHEN REGEXP_LIKE(o.client_number, '^[A-Z]') IS NOT TRUE
            THEN COALESCE(i.sales_org, o.sale_org)
            ELSE i.sales_org
        END AS sales_org_sub,
        CASE
            WHEN sub.trade_doc_number IS NOT NULL THEN sub.trade_doc_number
            WHEN i.trade_doc_number   IS NOT NULL THEN i.trade_doc_number
            WHEN o.sample_number      IS NOT NULL THEN o.sample_number
        END AS initial_order_number,
        CASE
            WHEN sub.trade_doc_pos IS NOT NULL THEN sub.trade_doc_pos
            WHEN i.trade_doc_pos   IS NOT NULL THEN i.trade_doc_pos
            WHEN o.sample_position IS NOT NULL THEN o.sample_position
        END AS initial_order_position_number,
        esol.id  AS id1,
        esol1.id AS id2,
        esol2.id AS id3,
        sub.site_id AS id4,
        o.*
    FROM orders o
    LEFT JOIN filtered_invoice i
        ON i.invoice_number  = COALESCE(o.sample_number, '')
       AND i.position_number = COALESCE(o.sample_position, '')
    LEFT JOIN preparation.P_GN_ORD_SALES_DEPT_SUB sub
        ON sub.sale_number   = o.sample_number
       AND sub.sale_position = o.sample_position
    LEFT JOIN site_order_link_prep esol
        ON esol.erp_sale_number  = o.sale_number
    LEFT JOIN site_order_link_prep esol1
        ON esol1.erp_sale_number = o.sample_number
    LEFT JOIN site_order_link_prep esol2
        ON esol2.erp_sale_number = i.trade_doc_number
),
change_sale_dep_return AS (
    SELECT
        sdp.*,
        CASE
            WHEN sdp.sales_department_imkc_prep IN ('1098','1099')
            THEN
        CASE
            WHEN MAX(
                    CASE
                        WHEN COALESCE(sdp.id1, sdp.id2, sdp.id3, sdp.id4) IS NULL
                        THEN 0
                        ELSE 1
                    END
                 ) OVER (PARTITION BY sdp.sale_number) = 1
            THEN '1098'
            ELSE '1099'
        END
            ELSE sdp.sales_department_imkc_prep
        END AS sales_department_imkc
    FROM change_sale_dep_return_prep sdp
),
orders_with_need_formats AS (
    SELECT
        s.*,
        esd.business_format  AS bsn_frmt,
        esd2.business_format AS bsn_frmt_imkc,
        oha.author           AS init_order_author,
        CASE
            WHEN s.return_type IS TRUE THEN -s.change_sign_flag
            ELSE s.change_sign_flag
        END AS change_sign_flag2,
        CASE WHEN esd2.business_format = 'SDDIS' THEN TRUE ELSE FALSE END AS tv_flg,
        CASE
            WHEN esd2.business_format IN ('NEWP','SDBIG','SDBRG','SDKOT','SDMOK',
                                          'IMKC','IM','KC','SDKR','SDSP')
              OR (esd2.business_format = 'TEPLO' AND s.sale_channel IN ('02','03','04'))
            THEN TRUE ELSE FALSE
        END AS sd_flg,
        CASE
            WHEN esd2.business_format IN ('SDSV','SDSZ','SDSI','SDSC','LOGV','SDSW','TEPLO')
            THEN TRUE ELSE FALSE
        END AS sk_flg,
        CASE WHEN esd2.business_format = 'SDOB' THEN TRUE ELSE FALSE END AS ob_flg,
        CASE
            WHEN s.sales_department_imkc = '6002' AND s.ordr_dt < DATE '2022-09-19' THEN FALSE
            WHEN esd2.business_format IN ('PHBL','PHVIN','VINS') THEN TRUE
            ELSE FALSE
        END AS ph_flg,
        eop.mp_order_number AS mp_ordr_nbr
    FROM change_sale_dep_return s
    LEFT JOIN erp_sales_department esd
        ON esd.sales_department = s.sale_department
    LEFT JOIN erp_sales_department esd2
        ON esd2.sales_department = s.sales_department_imkc
    LEFT JOIN order_header_author oha
        ON oha.sale_number = s.initial_order_number
    LEFT JOIN erp_order_payment eop
        ON eop.sale_number   = s.sale_number
       AND eop.sale_position = s.sale_position
       AND eop._deleted IS NOT TRUE
)
SELECT
    CAST(po.ordr_dt AS DATE) AS load_dt,
    COALESCE(d1.gn_grade_type_id, -1)               AS gn_grade_type_id,
    COALESCE(d2.gn_plant_id, -1)                    AS gn_plant_id,
    COALESCE(d3.gn_shipment_place_id, -1)           AS gn_shipment_place_id,
    COALESCE(d4.gn_sale_document_kind_id, -1)       AS gn_sale_document_kind_id,
    COALESCE(d5.gn_sales_organization_id, -1)       AS gn_sales_organization_id,
    COALESCE(d5.gn_sales_organization_id, -1)       AS gn_sales_organization_id1,
    COALESCE(d25.gn_sales_organization_id, -1)      AS gn_sales_organization_id2,
    COALESCE(d6.gn_sale_channel_id, -1)             AS gn_sale_channel_id,
    COALESCE(d7.gn_sales_department_id, -1)         AS gn_sales_department_id1,
    COALESCE(d8.gn_sales_department_id, -1)         AS gn_sales_department_id2,
    COALESCE(d9.gn_incoterms_id, -1)                AS gn_incoterms_id,
    COALESCE(d10.gn_debitor_id, -1)                 AS gn_debitor_id1,
    COALESCE(d11.gn_debitor_id, -1)                 AS gn_debitor_id2,
    COALESCE(d12.gn_product_id, -1)                 AS gn_product_id,
    COALESCE(d29.gn_product_id, -1)                 AS gn_product_id2,
    COALESCE(d13.gn_employee_id, -1)                AS gn_employee_id1,
    COALESCE(d14.gn_employee_id, -1)                AS gn_employee_id2,
    COALESCE(d15.gn_employee_id, -1)                AS gn_employee_id3,
    COALESCE(d16.gn_employee_id, -1)                AS gn_employee_id4,
    COALESCE(d17.gn_transaction_sap_id, -1)         AS gn_transaction_sap_id,
    COALESCE(d18.gn_sale_document_type_id, -1)      AS gn_sale_document_type_id,
    COALESCE(d19.gn_order_reason_id, -1)            AS gn_order_reason_id,
    COALESCE(d20.gn_rejection_reason_id, -1)        AS gn_rejection_reason_id,
    COALESCE(d21.gn_business_format_id, -1)         AS gn_business_format_id1,
    COALESCE(d22.gn_business_format_id, -1)         AS gn_business_format_id2,
    COALESCE(d23.gn_price_schema_id, -1)            AS gn_price_schema_id,
    COALESCE(d24.gn_position_type_id, -1)           AS gn_position_type_id,
    COALESCE(d26.gn_object_data_id, -1)             AS gn_object_data_id,
    COALESCE(d27.gn_employee_id, -1)                AS gn_employee_id5,
    CASE
        WHEN po.initial_order_number IS NULL
          OR po.hdr_samp_doc_kind = 'AG'
        THEN COALESCE(d13.gn_employee_id, -1)
        ELSE COALESCE(d28.gn_employee_id, -1)
    END AS gn_employee_id6,
    po.ordr_dt,
    po.ordr_pos_dt,
    po.sale_number                AS ordr_cd,
    po.sale_position              AS ordr_pos_nbr,
    po.hdr_samp_cd,
    po.sample_number              AS samp_cd,
    po.sample_position            AS samp_pos_nbr,
    po.mp_ordr_nbr,
    (po.net_cost       * po.change_sign_flag2) AS net_cost,
    (po.calculate_cost * po.change_sign_flag2) AS clc_cost,
    (po.tax            * po.change_sign_flag2) AS tax,
    po.fact_count                 AS prch_cnt,
    po.fact_count_store           AS base_cnt,
    po.is_brigadier               AS brgdr_f,
    po.telephone_number           AS tel_nbr,
    po.internet_order_number      AS site_ordr_cd,
    po.express_pickup             AS exprs_eqmt,
    po.contract                   AS cntrct,
    po.payment_type               AS pmnt_tp,
    po.mark_down_flg              AS mrkd_f,
    po.discount_card              AS dcn_card_nbr,
    po.gtr_number                 AS gtr_nbr,
    po.initial_order_number           AS init_ordr_nbr,
    po.initial_order_position_number  AS init_ordr_pos_nbr,
    po.promo_action               AS promo_actn,
    po.order_source               AS ordr_src_nm,
    po.local_payment_flag         AS lcl_pmnt_flg,
    po.tv_flg,
    po.sd_flg,
    po.sk_flg,
    po.ob_flg,
    po.ph_flg
FROM orders_with_need_formats po
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
LEFT JOIN prepared.D_GN_EMPLOYEE d16
    ON d16.gn_employee_cd = po.passanger
LEFT JOIN prepared.D_GN_TRANSACTION_SAP d17
    ON d17.gn_transaction_sap_cd = po.transaction_code
LEFT JOIN prepared.D_GN_SALE_DOCUMENT_TYPE d18
    ON d18.gn_sale_document_type_cd = po.sale_doc_dype
LEFT JOIN prepared.D_GN_ORDER_REASON d19
    ON d19.gn_order_reason_cd = po.ordr_rjct_rsn
LEFT JOIN prepared.D_GN_REJECTION_REASON d20
    ON d20.gn_rejection_reason_cd = po.ordr_pos_rjct_rsn
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT d21
    ON d21.gn_business_format_cd = po.bsn_frmt
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT d22
    ON d22.gn_business_format_cd = po.bsn_frmt_imkc
LEFT JOIN prepared.D_GN_PRICE_SCHEMA d23
    ON SPLIT_PART(d23.gn_price_schema_cd, '-', 1) = po.price_schema
   AND d23.co_usg = 'A'
   AND d23.co_appl = 'V'
LEFT JOIN prepared.D_GN_POSITION_TYPE d24
    ON d24.gn_position_type_cd = po.position_type
LEFT JOIN prepared.D_GN_SALES_ORGANIZATION d25
    ON d25.gn_sales_organization_cd = po.sales_org_sub
LEFT JOIN prepared.D_GN_OBJECT_DATA d26
    ON d26.gn_object_data_cd = po.project_id
LEFT JOIN prepared.D_GN_EMPLOYEE d27
    ON d27.gn_employee_cd = po.hdr_samp_author
LEFT JOIN prepared.D_GN_EMPLOYEE d28
    ON d28.gn_employee_cd = po.init_order_author
LEFT JOIN prepared.D_GN_PRODUCT d29
    ON d29.gn_product_cd = po.substitution_product
;
