with
    stg_endereco as (
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

    , join1 as (
        select *

        from stg_endereco 
        left join stg_estado_provincia on
            stg_endereco.id_provincia_estado = stg_estado_provincia.id_provincia_estado
        left join stg_regiao_pais on
             stg_estado_provincia.id_regiao_pais = stg_regiao_pais.id_regiao_pais
    )

    select 
        id_endereco
        , nome_cidade
        --, id_provincia_estado
        --, id_provincia_estado_1
        --, id_regiao_pais
        , eh_apenas_pais
        , nome_estado_provincia
        --, id_regiao_pais_1
        , nome_regiao_pais
    from join1