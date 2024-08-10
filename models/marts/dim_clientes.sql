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

    , joined_tabelas1_endereco_completo as(
        select 

        stg_endereco.id_endereco
        ,stg_endereco.nome_cidade
        --id_provincia_estado
        --id_provincia_estado_1
        --id_regiao_pais
        ,stg_estado_provincia.nome_estado_provincia
        --id_regiao_pais_1
        ,stg_regiao_pais.nome_regiao_pais
        ,stg_estado_provincia.eh_apenas_pais

        from stg_endereco
        left join stg_estado_provincia on
            stg_endereco.id_provincia_estado = stg_estado_provincia.id_provincia_estado
        left join stg_regiao_pais on
            stg_estado_provincia.id_regiao_pais = stg_regiao_pais.id_regiao_pais    
    )

    , joined_tabelas2_pessoa_endereco as(
        select 

        stg_pessoas.id_businessentity
        , stg_pessoas.tipo_pessoa
        , stg_pessoas.nome_completo
        --id_businessentity_1
        --id_endereco
        --id_endereco_1
        , joined_tabelas1_endereco_completo.nome_cidade
        , joined_tabelas1_endereco_completo.nome_estado_provincia
        , joined_tabelas1_endereco_completo.nome_regiao_pais
        , joined_tabelas1_endereco_completo.eh_apenas_pais

        from stg_pessoas
        left join stg_relacao_endereco_businessentity on
            stg_pessoas.id_businessentity = stg_relacao_endereco_businessentity.id_businessentity
        left join joined_tabelas1_endereco_completo on
            stg_relacao_endereco_businessentity.id_endereco = joined_tabelas1_endereco_completo.id_endereco
 
    )

    , joined_tabelas3_pessoaendereco_cliente as(
        select 

        id_cliente
        --,id_pessoa
        ,id_businessentity as id_businessentity_pessoa
        ,tipo_pessoa
        ,nome_completo
        ,nome_cidade
        ,nome_estado_provincia
        ,nome_regiao_pais
        ,eh_apenas_pais


        from joined_tabelas2_pessoa_endereco
        left join stg_clientes on
            joined_tabelas2_pessoa_endereco.id_businessentity = stg_clientes.id_pessoa
        where id_cliente is not null

    )

    select * 
from joined_tabelas3_pessoaendereco_cliente 