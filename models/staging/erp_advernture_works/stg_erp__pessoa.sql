with
    fonte_pessoa as (
        select  

        cast(businessentityid as integer) as id_businessentity
        ,cast(persontype as string) as tipo_pessoa
        --namestyle
        --title
        ,cast(firstname as string) || ' ' || cast(lastname as string) as nome_completo
        --middlename
        --suffix
        --emailpromotion
        --additionalcontactinfo
        --demographics
        --rowguid
        --modifieddate
        --cast(customerid as integer) as id_cliente
        --,cast(personid as integer) as id_pessoa

        from {{ source('erp adventure works', 'person_person') }}
    )

 select * 
 from fonte_pessoa  