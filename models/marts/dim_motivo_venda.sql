with    
    stg_motivo_venda as (
        select *
        from {{ ref('stg_erp__motivo_venda') }}
    )

    ,stg_motivo_venda_por_pedido as (
        select *
        from {{ ref('stg_erp__motivo_venda_por_pedido') }}
    )

    ,joined2_motivo_venda_motivo_venda_por_pedidos as (
        select 
        id_pedido
        ,nome_motivo_venda
        ,tipo_motivo_venda
      
        from stg_motivo_venda_por_pedido
        left join stg_motivo_venda on
            stg_motivo_venda_por_pedido.id_motivo_venda = stg_motivo_venda.id_motivo_venda
        )

select * 
 from joined2_motivo_venda_motivo_venda_por_pedidos