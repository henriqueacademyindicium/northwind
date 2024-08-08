with
    fonte_endereco as (
        select 

        cast(addressid as integer) as id_endereco
        --addressline1
        --addressline2
        , cast(city as string) as nome_cidade
        , cast(stateprovinceid as integer) as id_provincia_estado
        --postalcode
        --spatiallocation
        --rowguid
        --modifieddate

        from {{ source('erp adventure works', 'person_address') }}
    )

 select * 
 from fonte_endereco 
