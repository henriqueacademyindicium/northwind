with
    stg_pessoas as (
        select *
        from {{ ref('stg_erp__pessoa') }}
    )

    ,stg_clientes as (
        select *
        from {{ ref('stg_erp__clientes') }}
    )

    ,stg_relacao_endereco_businessentity as (
    select *
    from {{ ref('stg_erp__relacao_businessentityid_adressid') }}
    )
       
    ,stg_endereco as (
    select *
    from {{ ref('stg_erp__endereco') }}
    )

    ,stg_estado_provincia as (
    select *
    from {{ ref('stg_erp__estado_provincia') }}
    )

    ,stg_regiao_pais as (
    select *
    from {{ ref('stg_erp__regiao_pais') }}
    )

    , joined_tabelas1_ as(
        select *

        from stg_produtos
        left join stg_subcategorias on
            stg_produtos.id_subcategoria = stg_subcategorias.id_subcategoria    
    )

select * 
from stg_regiao_pais 