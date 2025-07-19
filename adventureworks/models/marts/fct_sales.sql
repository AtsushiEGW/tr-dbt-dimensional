with
stg_salesorderheader as (
    select
        salesorderid,
        customerid,
        creditcardid,
        shiptoaddressid,
        status as order_status,
        cast(orderdate as date) as orderdate
        from {{ ref('salesorderheader') }}
),

stg_salesorderdetail as (
    select
        salesorderid,
        salesorderdetailid,
        productid,
        orderqty,
        unitprice,
        unitprice * orderqty as revenue
    from {{ ref('salesorderdetail') }}
)

-- stg_salesorderheader は 1566rows
-- stg_salesorderdetail は 5716rows
select
    {{ dbt_utils.generate_surrogate_key(
        ['stg_salesorderdetail.salesorderid', 'salesorderdetailid']
        ) }} as sales_key,


    -- ディメンション結合のためのサロゲートキーを作成する

    {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['creditcardid']) }} as credit_card_key,
    {{ dbt_utils.generate_surrogate_key(['shiptoaddressid']) }} as ship_to_address_key,
    {{ dbt_utils.generate_surrogate_key(['order_status']) }} as order_status_key,
    {{ dbt_utils.generate_surrogate_key(['orderdate']) }} as order_date_key,

    -- その他のファクトテーブルの値
    stg_salesorderdetail.salesorderid,
    stg_salesorderdetail.salesorderdetailid,
    stg_salesorderdetail.unitprice,
    stg_salesorderdetail.orderqty,
    stg_salesorderdetail.revenue


from stg_salesorderdetail
inner join stg_salesorderheader
    on stg_salesorderdetail.salesorderid = stg_salesorderheader.salesorderid