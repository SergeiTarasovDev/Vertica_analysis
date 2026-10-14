--P_GN_ORDER
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
        eo.promo_action
    FROM public.ERP_ORDER eo
    LEFT JOIN public.ERP_ORDER_POSITIONS eop
        ON eop.sale_number = eo.sale_number
    WHERE
        eo.created_at >= '2018-01-01'
        AND CASE
                WHEN eo._processed_at > eop._processed_at
                THEN eo._processed_at
                ELSE eop._processed_at
            END IS NOT NULL
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
    WHERE
        esol._deleted IS NOT TRUE
),
erp_cashbook_payment AS (
    SELECT
        sale_number,
        cashbox,
        transaction_number,
        payment_number
    FROM public.ERP_PAYMENT_POSITION
    WHERE
        _processed_at IS NOT NULL
        AND _deleted = FALSE
),
erp_order_payment AS (
    SELECT
        sale_number,
        contract,
        shipping_payment_method
    FROM public.ERP_ORDER_PAYMENT
    WHERE
        _processed_at IS NOT NULL
        AND _deleted = FALSE
        AND sale_position = '00'
),
erp_sales_organization AS (
    SELECT
        sales_organization,
        business
    FROM public.ERP_SALES_ORGANIZATION
    WHERE
        _deleted IS NOT TRUE
),
erp_user_motivation AS (
    SELECT
        sale_number,
        user_name
    FROM public.ERP_USER_MOTIVATION
    WHERE
        _deleted IS NOT TRUE
),
erp_partner_position AS (
    SELECT
        document_number,
        debitor
    FROM public.ERP_PARTNER_POSITION
    WHERE
        _processed_at IS NOT NULL
        AND _deleted = FALSE
        AND partner_role = 'SH'
),
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
            WHEN express_pickup IS TRUE
            THEN 'Да'
            ELSE 'Нет'
        END AS express_pickup
    FROM erp_site_link_order
    WHERE
        business <> 'ENRG'
),
payment AS (
    SELECT
        sale_number,
        cashbox,
        transaction_number::!INT AS transaction_number,
        ROW_NUMBER() OVER (
            PARTITION BY sale_number
            ORDER BY payment_number DESC NULLS LAST
        ) AS payment_rank
    FROM erp_cashbook_payment
    WHERE
        sale_number IS NOT NULL
        AND (
            transaction_number::!INT IN (30, 31, 4, 5, 100, 101)
            OR cashbox IN ('9851', '9852', '9853', '9854')
        )
)
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
    (CASE
        WHEN p.cashbox = '9851'                 THEN 'Оплата по ссылке'
        WHEN p.cashbox = '9852'                 THEN 'По СБП/QR-динамическому'
        WHEN p.cashbox = '9853'                 THEN 'Оплата частями'
        WHEN p.cashbox = '9854'                 THEN 'СБП B2B'
        WHEN p.transaction_number IN (4, 5)     THEN 'Оплата наличными'
        WHEN p.transaction_number IN (30, 31)   THEN 'Оплата по терминалу'
        WHEN p.transaction_number IN (100, 101) THEN 'Оплата по QR-табличке'
        WHEN dd.payment_type = 'invoice'        THEN 'Cписанием со счета (ЮЛ)'
    END)                   AS payment_type,
    oh.order_source        AS order_source,
    CASE
        WHEN eop.shipping_payment_method = 'PT01'
        THEN TRUE
        ELSE FALSE
    END                                 AS local_payment_flag
FROM order_header oh
INNER JOIN erp_order_payment eop
    ON eop.sale_number = oh.sale_number
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
LEFT JOIN payment p
    ON p.sale_number = oh.sale_number
    AND p.payment_rank = 1
WHERE
    oh.sale_doc_kind <> 'AG'
    AND oh.sale_doc_dype IN ('B', 'C', 'H', 'K')
    AND (eso.business <> 'ENRG' OR eso.business IS NULL)
