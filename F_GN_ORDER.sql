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
)
ORDER BY
    ordr_cd,
    ordr_pos_nbr
PARTITION BY
    load_dt::DATE
GROUP BY
    DATE_TRUNC('MONTH', load_dt::DATE);



INSERT INTO sandbox_analytics.F_GN_ORDER
WITH erp_order AS
(
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
        eop.sample_number AS sample_number_position,
        eop.sample_position,
        eop.return_type AS return_type_position,
        eop.created_at AS created_at_position,
        eop.grade_type,
        eop.plant,
        eop.warehouse,
        eop.product_item,
        eop.substitution_product,
        eop.author AS author_position,
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
            WHEN
                eo._processed_at > eop._processed_at
            THEN
                eo._processed_at
            ELSE
                eop._processed_at
        END AS _processed_at,
        CASE
            WHEN
                eo._deleted IS TRUE
                OR eop._deleted IS TRUE
            THEN
                TRUE
            ELSE
                FALSE
        END AS _deleted
    FROM public.ERP_ORDER AS eo
    LEFT JOIN public.ERP_ORDER_POSITIONS AS eop
        ON eop.sale_number = eo.sale_number
    WHERE
        eo.created_at >= DATE '2018-01-01'
        AND eo._deleted IS NOT TRUE
        AND eop._deleted IS NOT TRUE
),
erp_site_link_order AS
(
    SELECT
        eso.id,
        eso.internet_order_number,
        eso.payment_type,
        esol.erp_sale_number,
        eso.business,
        esod.express_pickup
    FROM public.ERP_SITE_ORDER_LINK AS esol
    LEFT JOIN public.ERP_SITE_ORDER AS eso
        ON eso.id = esol.id
            AND eso._deleted IS NOT TRUE
    LEFT JOIN public.ERP_SITE_ORDER_DELIVERIES AS esod
        ON esod.order_id = eso.id
            AND esod._deleted IS NOT TRUE
    WHERE
        esol._deleted IS NOT TRUE
),
erp_invoice AS
(
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
        ei.payer LIKE 'T%' AS payer_startwith_t_flg,
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
            WHEN
                ei._processed_at > eip._processed_at
            THEN
                ei._processed_at
            ELSE
                eip._processed_at
        END AS _processed_at,
        CASE
            WHEN
                ei._deleted IS TRUE
                OR eip._deleted IS TRUE
            THEN
                TRUE
            ELSE
                FALSE
        END AS _deleted
    FROM public.ERP_INVOICE AS ei
    LEFT JOIN public.ERP_INVOICE_POSITIONS AS eip
        ON eip.invoice_number = ei.invoice_number
    WHERE
        ei._deleted IS NOT TRUE
        AND eip._deleted IS NOT TRUE
),
erp_sales_department AS
(
    SELECT
        *
    FROM public.ERP_SALES_DEPARTMENT AS esd
    WHERE
        esd._deleted IS NOT TRUE
),
erp_order_payment AS
(
    SELECT
        *
    FROM public.ERP_ORDER_PAYMENT AS eop
    WHERE
        eop._deleted IS NOT TRUE
),
order_header_author AS
(
    SELECT
        eo.sale_number,
        eo.author
    FROM public.ERP_ORDER AS eo
),
i_gn_debitor_exceptions AS
(
    SELECT
        igde.gn_debitor_cd
    FROM preparation.I_GN_DEBITOR_EXCEPTIONS AS igde
),
filtered_invoice AS
(
    SELECT
        ei.*
    FROM erp_invoice AS ei
    WHERE
        ei.client_number NOT IN (
            SELECT
                igde.gn_debitor_cd
            FROM i_gn_debitor_exceptions AS igde
        )
),
orders AS
(
    SELECT
        pgo.ord_dt AS ordr_dt,
        pgo.sale_doc_kind,
        pgo.sale_org,
        pgo.sale_channel,
        pgo.sale_department,
        pgo.incoterms,
        pgo.client_number,
        pgo.author AS ordr_ahr,
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
        eo.return_type_position AS return_type,
        eo.position_type,
        eo.mark_down_flg,
        eo.gtr_number,
        eo.sale_doc_kind AS ordr_doc_kind,
        CASE
            WHEN
                (
                    pgo.sale_doc_dype IN ('H', 'K', 'N', 'O', 'T', '6')
                    AND eo.return_type_position IS NOT TRUE
                )
                OR (
                    pgo.sale_doc_dype NOT IN ('H', 'K', 'N', 'O', 'T', '6')
                    AND eo.return_type_position IS TRUE
                )
            THEN
                -1
            ELSE
                1
        END AS change_sign_flag,
        CASE
            WHEN
                eo.return_type_position IS TRUE
            THEN
                -eo.fact_count
            ELSE
                eo.fact_count
        END AS fact_count,
        CASE
            WHEN
                eo.return_type_position IS TRUE
            THEN
                -eo.fact_count_store
            ELSE
                eo.fact_count_store
        END AS fact_count_store,
        pgo.order_source,
        pgo.local_payment_flag
    FROM preparation.P_GN_ORDER AS pgo
    INNER JOIN erp_order AS eo
        ON eo.sale_number = pgo.sale_number
    WHERE
        eo.sale_position IS NOT NULL
        AND (
            eo.plant NOT IN (
                '1918', '1919', '1928',
                '1929', '1938', '1939',
                '1948', '1949', '3999'
            )
            OR eo.plant IS NULL
        )
),
site_order_link_prep AS
(
    SELECT
        esol.id,
        esol.erp_sale_number
    FROM erp_site_link_order AS esol
    GROUP BY
        esol.id,
        esol.erp_sale_number
),
change_sale_dep_return_prep AS
(
    SELECT
        CASE
            WHEN
                REGEXP_LIKE(o.client_number, '^[A-Z]') IS NOT TRUE
            THEN
                CASE
                    WHEN
                        o.sale_department = '1021'
                        AND o.sale_channel = '01'
                    THEN
                        '1020'
                    ELSE
                        CASE
                            WHEN
                                fi.sales_department IS NOT NULL
                            THEN
                                CASE
                                    WHEN
                                        fi.sales_department = '1021'
                                        AND fi.sales_channel = '01'
                                    THEN
                                        '1020'
                                    ELSE
                                        fi.sales_department
                                END
                            ELSE
                                o.sale_department
                        END
                END
            ELSE
                o.sale_department
        END AS sales_department_imkc_prep,
        CASE
            WHEN
                REGEXP_LIKE(o.client_number, '^[A-Z]') IS NOT TRUE
            THEN
                COALESCE(fi.sales_org, o.sale_org)
            ELSE
                fi.sales_org
        END AS sales_org_sub,
        CASE
            WHEN
                pgosds.trade_doc_number IS NOT NULL
            THEN
                pgosds.trade_doc_number
            WHEN
                fi.trade_doc_number IS NOT NULL
            THEN
                fi.trade_doc_number
            WHEN
                o.sample_number IS NOT NULL
            THEN
                o.sample_number
        END AS initial_order_number,
        CASE
            WHEN
                pgosds.trade_doc_pos IS NOT NULL
            THEN
                pgosds.trade_doc_pos
            WHEN
                fi.trade_doc_pos IS NOT NULL
            THEN
                fi.trade_doc_pos
            WHEN
                o.sample_position IS NOT NULL
            THEN
                o.sample_position
        END AS initial_order_position_number,
        solp.id AS id1,
        solp2.id AS id2,
        solp3.id AS id3,
        pgosds.site_id AS id4,
        o.*
    FROM orders AS o
    LEFT JOIN filtered_invoice AS fi
        ON fi.invoice_number = COALESCE(o.sample_number, '')
            AND fi.position_number = COALESCE(o.sample_position, '')
    LEFT JOIN preparation.P_GN_ORD_SALES_DEPT_SUB AS pgosds
        ON pgosds.sale_number = o.sample_number
            AND pgosds.sale_position = o.sample_position
    LEFT JOIN site_order_link_prep AS solp
        ON solp.erp_sale_number = o.sale_number
    LEFT JOIN site_order_link_prep AS solp2
        ON solp2.erp_sale_number = o.sample_number
    LEFT JOIN site_order_link_prep AS solp3
        ON solp3.erp_sale_number = fi.trade_doc_number
),
change_sale_dep_return AS
(
    SELECT
        csdrp.*,
        CASE
            WHEN
                csdrp.sales_department_imkc_prep IN ('1098', '1099')
            THEN
                CASE
                    WHEN
                        MAX(
                            CASE
                                WHEN
                                    COALESCE(csdrp.id1, csdrp.id2, csdrp.id3, csdrp.id4) IS NULL
                                THEN
                                    0
                                ELSE
                                    1
                            END
                        ) OVER (
                            PARTITION BY csdrp.sale_number
                        ) = 1
                    THEN
                        '1098'
                    ELSE
                        '1099'
                END
            ELSE
                csdrp.sales_department_imkc_prep
        END AS sales_department_imkc
    FROM change_sale_dep_return_prep AS csdrp
),
orders_with_need_formats AS
(
    SELECT
        csdr.*,
        esd.business_format AS bsn_frmt,
        esd2.business_format AS bsn_frmt_imkc,
        oha.author AS init_order_author,
        CASE
            WHEN
                csdr.return_type IS TRUE
            THEN
                -csdr.change_sign_flag
            ELSE
                csdr.change_sign_flag
        END AS change_sign_flag2,
        CASE
            WHEN
                esd2.business_format = 'SDDIS'
            THEN
                TRUE
            ELSE
                FALSE
        END AS tv_flg,
        CASE
            WHEN
                esd2.business_format IN (
                    'NEWP', 'SDBIG', 'SDBRG',
                    'SDKOT', 'SDMOK', 'IMKC',
                    'IM', 'KC', 'SDKR',
                    'SDSP'
                )
                OR (
                    esd2.business_format IN ('TEPLO')
                    AND csdr.sale_channel IN ('02', '03', '04')
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS sd_flg,
        CASE
            WHEN
                esd2.business_format IN (
                    'SDSV', 'SDSZ', 'SDSI',
                    'SDSC', 'LOGV', 'SDSW',
                    'TEPLO'
                )
            THEN
                TRUE
            ELSE
                FALSE
        END AS sk_flg,
        CASE
            WHEN
                esd2.business_format = 'SDOB'
            THEN
                TRUE
            ELSE
                FALSE
        END AS ob_flg,
        CASE
            WHEN
                csdr.sales_department_imkc = '6002'
                AND csdr.ordr_dt < DATE '2022-09-19'
            THEN
                FALSE
            WHEN
                esd2.business_format IN ('PHBL', 'PHVIN', 'VINS')
            THEN
                TRUE
            ELSE
                FALSE
        END AS ph_flg,
        eop.mp_order_number AS mp_ordr_nbr
    FROM change_sale_dep_return AS csdr
    LEFT JOIN erp_sales_department AS esd
        ON esd.sales_department = csdr.sale_department
    LEFT JOIN erp_sales_department AS esd2
        ON esd2.sales_department = csdr.sales_department_imkc
    LEFT JOIN order_header_author AS oha
        ON oha.sale_number = csdr.initial_order_number
    LEFT JOIN erp_order_payment AS eop
        ON eop.sale_number = csdr.sale_number
            AND eop.sale_position = csdr.sale_position
            AND eop._deleted IS NOT TRUE
)
SELECT
    CAST(ownf.ordr_dt AS DATE) AS load_dt,
    COALESCE(dggt.gn_grade_type_id, -1) AS gn_grade_type_id,
    COALESCE(dgp.gn_plant_id, -1) AS gn_plant_id,
    COALESCE(dgsp.gn_shipment_place_id, -1) AS gn_shipment_place_id,
    COALESCE(dgsdk.gn_sale_document_kind_id, -1) AS gn_sale_document_kind_id,
    COALESCE(dgso.gn_sales_organization_id, -1) AS gn_sales_organization_id,
    COALESCE(dgso.gn_sales_organization_id, -1) AS gn_sales_organization_id1,
    COALESCE(dgso2.gn_sales_organization_id, -1) AS gn_sales_organization_id2,
    COALESCE(dgsc.gn_sale_channel_id, -1) AS gn_sale_channel_id,
    COALESCE(dgsd.gn_sales_department_id, -1) AS gn_sales_department_id1,
    COALESCE(dgsd2.gn_sales_department_id, -1) AS gn_sales_department_id2,
    COALESCE(dgi.gn_incoterms_id, -1) AS gn_incoterms_id,
    COALESCE(dgd.gn_debitor_id, -1) AS gn_debitor_id1,
    COALESCE(dgd2.gn_debitor_id, -1) AS gn_debitor_id2,
    COALESCE(dgp2.gn_product_id, -1) AS gn_product_id,
    COALESCE(dgp3.gn_product_id, -1) AS gn_product_id2,
    COALESCE(dge.gn_employee_id, -1) AS gn_employee_id1,
    COALESCE(dge2.gn_employee_id, -1) AS gn_employee_id2,
    COALESCE(dge3.gn_employee_id, -1) AS gn_employee_id3,
    COALESCE(dge4.gn_employee_id, -1) AS gn_employee_id4,
    COALESCE(dgts.gn_transaction_sap_id, -1) AS gn_transaction_sap_id,
    COALESCE(dgsdt.gn_sale_document_type_id, -1) AS gn_sale_document_type_id,
    COALESCE(dgor.gn_order_reason_id, -1) AS gn_order_reason_id,
    COALESCE(dgrr.gn_rejection_reason_id, -1) AS gn_rejection_reason_id,
    COALESCE(dgbf.gn_business_format_id, -1) AS gn_business_format_id1,
    COALESCE(dgbf2.gn_business_format_id, -1) AS gn_business_format_id2,
    COALESCE(dgps.gn_price_schema_id, -1) AS gn_price_schema_id,
    COALESCE(dgpt.gn_position_type_id, -1) AS gn_position_type_id,
    COALESCE(dgod.gn_object_data_id, -1) AS gn_object_data_id,
    COALESCE(dge5.gn_employee_id, -1) AS gn_employee_id5,
    CASE
        WHEN
            ownf.initial_order_number IS NULL
            OR ownf.hdr_samp_doc_kind = 'AG'
        THEN
            COALESCE(dge.gn_employee_id, -1)
        ELSE
            COALESCE(dge6.gn_employee_id, -1)
    END AS gn_employee_id6,
    ownf.ordr_dt AS ordr_dt,
    ownf.ordr_pos_dt AS ordr_pos_dt,
    ownf.sale_number AS ordr_cd,
    ownf.sale_position AS ordr_pos_nbr,
    ownf.hdr_samp_cd,
    ownf.sample_number AS samp_cd,
    ownf.sample_position AS samp_pos_nbr,
    ownf.mp_ordr_nbr,
    (ownf.net_cost * ownf.change_sign_flag2) AS net_cost,
    (ownf.calculate_cost * ownf.change_sign_flag2) AS clc_cost,
    (ownf.tax * ownf.change_sign_flag2) AS tax,
    ownf.fact_count AS prch_cnt,
    ownf.fact_count_store AS base_cnt,
    ownf.is_brigadier AS brgdr_f,
    ownf.telephone_number AS tel_nbr,
    ownf.internet_order_number AS site_ordr_cd,
    ownf.express_pickup AS exprs_eqmt,
    ownf.contract AS cntrct,
    ownf.payment_type AS pmnt_tp,
    ownf.mark_down_flg AS mrkd_f,
    ownf.discount_card AS dcn_card_nbr,
    ownf.gtr_number AS gtr_nbr,
    ownf.initial_order_number AS init_ordr_nbr,
    ownf.initial_order_position_number AS init_ordr_pos_nbr,
    ownf.promo_action AS promo_actn,
    ownf.order_source AS ordr_src_nm,
    ownf.local_payment_flag AS lcl_pmnt_flg,
    ownf.tv_flg,
    ownf.sd_flg,
    ownf.sk_flg,
    ownf.ob_flg,
    ownf.ph_flg
FROM orders_with_need_formats AS ownf
LEFT JOIN prepared.D_GN_GRADE_TYPE AS dggt
    ON dggt.gn_grade_type_cd = ownf.grade_type
LEFT JOIN prepared.D_GN_PLANT AS dgp
    ON dgp.gn_plant_cd = ownf.plant
LEFT JOIN prepared.D_GN_SHIPMENT_PLACE AS dgsp
    ON dgsp.gn_shipment_place_cd = ownf.warehouse || ownf.plant
LEFT JOIN prepared.D_GN_SALE_DOCUMENT_KIND AS dgsdk
    ON dgsdk.gn_sale_document_kind_cd = ownf.sale_doc_kind
LEFT JOIN prepared.D_GN_SALES_ORGANIZATION AS dgso
    ON dgso.gn_sales_organization_cd = ownf.sale_org
LEFT JOIN prepared.D_GN_SALE_CHANNEL AS dgsc
    ON dgsc.gn_sale_channel_cd = ownf.sale_channel
LEFT JOIN prepared.D_GN_SALES_DEPARTMENT AS dgsd
    ON dgsd.gn_sales_department_cd = ownf.sale_department
LEFT JOIN prepared.D_GN_SALES_DEPARTMENT AS dgsd2
    ON dgsd2.gn_sales_department_cd = ownf.sales_department_imkc
LEFT JOIN prepared.D_GN_INCOTERMS AS dgi
    ON dgi.gn_incoterms_cd = ownf.incoterms
LEFT JOIN prepared.D_GN_DEBITOR AS dgd
    ON dgd.gn_debitor_cd = ownf.client_number
LEFT JOIN prepared.D_GN_DEBITOR AS dgd2
    ON dgd2.gn_debitor_cd = ownf.debitor_sale
LEFT JOIN prepared.D_GN_PRODUCT AS dgp2
    ON dgp2.gn_product_cd = ownf.product_item
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge
    ON dge.gn_employee_cd = ownf.ordr_ahr
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge2
    ON dge2.gn_employee_cd = ownf.ordr_ahr_hdr
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge3
    ON dge3.gn_employee_cd = ownf.sales_manager
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge4
    ON dge4.gn_employee_cd = ownf.passanger
LEFT JOIN prepared.D_GN_TRANSACTION_SAP AS dgts
    ON dgts.gn_transaction_sap_cd = ownf.transaction_code
LEFT JOIN prepared.D_GN_SALE_DOCUMENT_TYPE AS dgsdt
    ON dgsdt.gn_sale_document_type_cd = ownf.sale_doc_dype
LEFT JOIN prepared.D_GN_ORDER_REASON AS dgor
    ON dgor.gn_order_reason_cd = ownf.ordr_rjct_rsn
LEFT JOIN prepared.D_GN_REJECTION_REASON AS dgrr
    ON dgrr.gn_rejection_reason_cd = ownf.ordr_pos_rjct_rsn
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT AS dgbf
    ON dgbf.gn_business_format_cd = ownf.bsn_frmt
LEFT JOIN prepared.D_GN_BUSINESS_FORMAT AS dgbf2
    ON dgbf2.gn_business_format_cd = ownf.bsn_frmt_imkc
LEFT JOIN prepared.D_GN_PRICE_SCHEMA AS dgps
    ON SPLIT_PART(dgps.gn_price_schema_cd, '-', 1) = ownf.price_schema
        AND dgps.co_usg = 'A'
        AND dgps.co_appl = 'V'
LEFT JOIN prepared.D_GN_POSITION_TYPE AS dgpt
    ON dgpt.gn_position_type_cd = ownf.position_type
LEFT JOIN prepared.D_GN_SALES_ORGANIZATION AS dgso2
    ON dgso2.gn_sales_organization_cd = ownf.sales_org_sub
LEFT JOIN prepared.D_GN_OBJECT_DATA AS dgod
    ON dgod.gn_object_data_cd = ownf.project_id
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge5
    ON dge5.gn_employee_cd = ownf.hdr_samp_author
LEFT JOIN prepared.D_GN_EMPLOYEE AS dge6
    ON dge6.gn_employee_cd = ownf.init_order_author
LEFT JOIN prepared.D_GN_PRODUCT AS dgp3
    ON dgp3.gn_product_cd = ownf.substitution_product;
