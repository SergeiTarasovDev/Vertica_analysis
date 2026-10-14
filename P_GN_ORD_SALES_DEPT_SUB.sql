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
    WHERE eo.created_at >= '2018-01-01'
    AND CASE
              WHEN eo._processed_at > eop._processed_at THEN eo._processed_at
              ELSE eop._processed_at
          END IS NOT NULL
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
        WHERE CASE
              WHEN ei._processed_at > eip._processed_at THEN ei._processed_at
              ELSE eip._processed_at
          END IS NOT NULL
),
i_gn_debitor_exceptions AS (
    SELECT gn_debitor_cd FROM preparation.I_GN_DEBITOR_EXCEPTIONS
),
filtered_invoice AS (
    SELECT *
    FROM erp_invoice
    WHERE client_number NOT IN (SELECT gn_debitor_cd FROM i_gn_debitor_exceptions)
),
erp_site_link_order_prep AS (
    SELECT
        erp_sale_number,
        id
    FROM erp_site_link_order
    GROUP BY erp_sale_number, id
),
order_first AS (
    SELECT
        created_at,
        sale_number,
        sale_position,
        sample_number_position AS sample_number,
        sample_position
    FROM erp_order
    WHERE sale_position IS NOT NULL
)
SELECT
    o.created_at AS load_dt,
    o.sale_number,
    o.sale_position,
    eslo.id      AS site_id,
    ei.trade_doc_number,
    ei.trade_doc_pos
FROM order_first o
LEFT JOIN filtered_invoice ei
    ON ei.invoice_number  = o.sample_number
   AND ei.position_number = o.sample_position
LEFT JOIN erp_site_link_order_prep eslo
    ON eslo.erp_sale_number = ei.trade_doc_number
ORDER BY o.sale_number, o.sale_position
;