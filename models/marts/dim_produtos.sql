with
    stg_categorias as (
        select *
        from {{ ref('stg_erp__categorias_produtos') }}
    )

    ,stg_subcategorias as (
        select *
        from {{ ref('stg_erp__subcategorias_produtos') }}
    )

    ,stg_produtos as (
        select *
        from {{ ref('stg_erp__produtos') }}
    )

    , joined_tabelas1 as(
        select 
            stg_produtos.id_produto
            , stg_produtos.nome_produto
            , stg_produtos.eh_manufaturado
            , stg_produtos.eh_produto_final
            , stg_produtos.id_subcategoria

            , stg_subcategorias.id_categoria
            , stg_subcategorias.nome_subcategoria
        from stg_produtos
        left join stg_subcategorias on
            stg_produtos.id_subcategoria = stg_subcategorias.id_subcategoria    
    )

    , joined_tabelas2 as (
        select
            joined_tabelas1.id_produto
            , joined_tabelas1.nome_produto
            , joined_tabelas1.eh_manufaturado
            , joined_tabelas1.eh_produto_final
            , joined_tabelas1.id_subcategoria

            , joined_tabelas1.id_categoria
            , joined_tabelas1.nome_subcategoria

            , stg_categorias.nome_categoria
        from joined_tabelas1
        left join stg_categorias on
            joined_tabelas1.id_categoria=stg_categorias.id_categoria 
    )

     select * 
 from joined_tabelas2 
  
 

