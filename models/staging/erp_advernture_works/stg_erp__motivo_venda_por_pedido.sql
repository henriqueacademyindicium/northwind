with
    fonte_motivo_vendas_por_pedido as (
        select 
            cast (salesorderid as int) as id_pedido
            ,cast (salesreasonid as int) as id_motivo_venda
            ,-- modifieddate

        from {{ source('erp adventure works', 'sales_salesorderheadersalesreason') }}
    )

 select * 
 from fonte_motivo_vendas_por_pedido   