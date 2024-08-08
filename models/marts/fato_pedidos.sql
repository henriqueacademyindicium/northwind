with
    stg_cartao_de_credito as (
        select *
        from {{ ref('stg_erp__cartao_de_credito') }}
    )

    ,stg_detalhe_pedidos as (
        select *
        from {{ ref('stg_erp__detalhe_pedidos') }}
    )

    ,stg_motivo_venda as (
        select *
        from {{ ref('stg_erp__motivo_venda') }}
    )

    ,stg_motivo_venda_por_pedido as (
        select *
        from {{ ref('stg_erp__motivo_venda_por_pedido') }}
    )

    ,stg_ofertas_especiais as (
        select *
        from {{ ref('stg_erp__ofertas_especiais') }}
    )

    ,stg_pedidos as (
        select *
        from {{ ref('stg_erp__pedidos') }}
    )

    ,joined1_detalhe_pedidos_ofertas_speciais as (
        select 
        id_pedido
        ,id_detalhe_pedido
        ,qtdade_pedido
        ,id_produto
        ,preco_unitario
        ,desconto_unitario
        ,nome_oferta_especial
        ,porcentagem_desconto
        ,tipo_oferta_especial
        ,categoria_oferta_especial

        from stg_detalhe_pedidos
        left join stg_ofertas_especiais on
            stg_detalhe_pedidos.id_oferta_especial = stg_ofertas_especiais.id_oferta_especial
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

    ,joined3_pedidos_joined1_joined2_cartao_de_credito as (
        select 

        stg_pedidos.id_pedido
        ,stg_pedidos.data_pedido
        ,stg_pedidos.data_entrega
        ,stg_pedidos.id_status
        ,stg_pedidos.eh_compra_online
        ,stg_pedidos.id_cliente
        ,stg_pedidos.id_cartao_de_credito
        ,stg_pedidos.subtotal
        ,stg_pedidos.taxas
        ,stg_pedidos.frete
        ,stg_pedidos.total
        ,joined1_detalhe_pedidos_ofertas_speciais.id_detalhe_pedido
        ,joined1_detalhe_pedidos_ofertas_speciais.qtdade_pedido
        ,joined1_detalhe_pedidos_ofertas_speciais.id_produto
        ,joined1_detalhe_pedidos_ofertas_speciais.preco_unitario
        ,joined1_detalhe_pedidos_ofertas_speciais.desconto_unitario
        ,joined1_detalhe_pedidos_ofertas_speciais.nome_oferta_especial
        ,joined1_detalhe_pedidos_ofertas_speciais.porcentagem_desconto
        ,joined1_detalhe_pedidos_ofertas_speciais.tipo_oferta_especial
        ,joined1_detalhe_pedidos_ofertas_speciais.categoria_oferta_especial
        ,joined2_motivo_venda_motivo_venda_por_pedidos.nome_motivo_venda
        ,joined2_motivo_venda_motivo_venda_por_pedidos.tipo_motivo_venda
        ,stg_cartao_de_credito.tipo_cartao_de_credito


        
        from stg_pedidos
        left join joined1_detalhe_pedidos_ofertas_speciais on
            stg_pedidos.id_pedido = joined1_detalhe_pedidos_ofertas_speciais.id_pedido
        left join joined2_motivo_venda_motivo_venda_por_pedidos on
            stg_pedidos.id_pedido = joined2_motivo_venda_motivo_venda_por_pedidos.id_pedido
        left join stg_cartao_de_credito on
            stg_pedidos.id_cartao_de_credito = stg_cartao_de_credito.id_cartao_de_credito
        )

     select * 
 from joined3_pedidos_joined1_joined2_cartao_de_credito

  
