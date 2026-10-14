--P_GN_INV_SALES_DEPT_SUB
WITH
invoice AS (
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
                WHEN ei._processed_at  eip._processed_at
                THEN ei._processed_at
                ELSE eip._processed_at
            END AS _processed_at,
            CASE
                WHEN ei._deleted IS TRUE OR eip._deleted IS TRUE
                THEN TRUE
                ELSE FALSE
            END AS _deleted
    FROM public.ERP_INVOICE AS ei
    LEFT JOIN public.ERP_INVOICE_POSITIONS AS eip
            ON ei.invoice_number = eip.invoice_number
	WHERE
		ei._deleted IS NOT TRUE
		AND eip._deleted IS NOT TRUE
),
sale_order AS (
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
            eop.sample_number as sample_number_position,
            eop.sample_position,
            eop.return_type as return_type_position,
            eop.created_at as created_at_position,
            eop.grade_type,
            eop.plant,
            eop.warehouse,
            eop.product_item,
            eop.substitution_product,
            eop.author as author_position,
            eop.net_cost,
            eop.calculate_cost,
            eop.tax,
            eop.rejection_reason as rejection_reason_position,
            eop.position_type,
            eop.mark_down_flg,
            eop.gtr_number,
            eop.fact_count,
            eop.fact_count_store,
            eop.guid,
            CASE
                WHEN eo._processed_at  eop._processed_at
                THEN eo._processed_at
                ELSE eop._processed_at
            END AS _processed_at,
            CASE
                WHEN eo._deleted IS TRUE OR eop._deleted IS TRUE
                THEN TRUE
                ELSE FALSE
            END AS _deleted
        FROM
            public.ERP_ORDER eo
        LEFT JOIN public.ERP_ORDER_POSITIONS eop
            ON eop.sale_number = eo.sale_number
	WHERE
		eo._deleted IS NOT TRUE
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
        WHERE
            esol._deleted IS NOT TRUE
),
i_gn_debitor_exceptions AS (
    SELECT gn_debitor_cd
    FROM preparation.I_GN_DEBITOR_EXCEPTIONS
),
filtered_invoice AS (
    SELECT
        FROM invoice
        WHERE client_number NOT IN (SELECT gn_debitor_cd FROM i_gn_debitor_exceptions)
),
erp_invoice AS (
    SELECT
            invoice_number,
            position_number,
            sales_department,
            sales_channel,
            sales_org,
            trade_doc_number
        FROM filtered_invoice
),
erp_order AS (
    SELECT
            created_at AS load_dt,
            sale_doc_kind,
            sale_number,
            sale_position,
            sample_number_position AS sample_number,
            sample_position
        FROM sale_order
),
order_first AS (
    SELECT
            eo.load_dt,
            eo.sale_doc_kind,
            eo.sale_number AS order_sale_number,
            eo.sale_position AS order_sale_position,
            eo.sample_number,
            eo.sample_position,
            slo.id AS site_id1
        FROM erp_order eo
        LEFT JOIN erp_site_link_order slo
            ON slo.erp_sale_number = eo.sample_number
        WHERE
            eo.sample_number IS NOT NULL
            AND eo.sample_position IS NOT NULL
),
order_second AS (
    SELECT
            oo.load_dt,
            oo.sale_doc_kind,
            oo.order_sale_number,
            oo.order_sale_position,
            oo.site_id1,
            ip.invoice_number,
            ip.position_number,
            ip.sales_department,
            ip.sales_channel,
            ip.sales_org,
            eop2.sale_number,
            eop2.sale_position,
            eop2.sample_number AS sn2,
            eop2.sample_position AS sp2,
            esol2.id AS site_id2
        FROM order_first oo
        LEFT JOIN erp_invoice ip
            ON oo.sample_number = ip.invoice_number
            AND oo.sample_position = ip.position_number
        LEFT JOIN erp_order eop2
            ON eop2.sale_number = oo.sample_number
            AND eop2.sale_position = oo.sample_position
        LEFT JOIN erp_site_link_order esol2
            ON esol2.erp_sale_number = ip.trade_doc_number
)
SELECT
    load_dtDATE AS load_dt,
    sale_doc_kindVARCHAR(4) AS sale_doc_kind,
    order_sale_numberVARCHAR(10) AS order_sale_number,
    order_sale_positionVARCHAR(6) AS order_sale_position,
    sales_departmentVARCHAR(4) AS sales_department,
    sales_channelVARCHAR(2) AS sales_channel,
    sales_orgVARCHAR(4) AS sales_org,
    site_id1INTEGER AS site_id1,
    site_id2INTEGER AS site_id2,
    site_id3INTEGER AS site_id3
FROM (
	SELECT
		oo2.load_dt,
		oo2.sale_doc_kind,
		oo2.order_sale_number,
		oo2.order_sale_position,
		oo2.sales_department,
		oo2.sales_channel,
		oo2.sales_org,
		oo2.site_id1,
		oo2.site_id2,
		esol3.id AS site_id3
	FROM order_second oo2
	LEFT JOIN erp_invoice ip2
		ON oo2.sn2 = ip2.invoice_number
		AND oo2.sp2 = ip2.position_number
	LEFT JOIN erp_site_link_order esol3
		ON esol3.erp_sale_number = ip2.trade_doc_number
	UNION ALL
	SELECT
		eo.load_dt,
		eo.sale_doc_kind,
		eo.sale_number AS order_sale_number,
		eo.sale_position AS order_sale_position,
		NULL AS sales_department,
		NULL AS sales_channel,
		NULL AS sales_org,
		NULL AS site_id1,
		NULL AS site_id2,
		NULL AS site_id3
	FROM erp_order eo
	WHERE
		eo.sample_number IS NULL
		AND eo.sample_position IS NULL
) AS result
