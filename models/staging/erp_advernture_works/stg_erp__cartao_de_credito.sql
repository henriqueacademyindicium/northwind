with
    fonte_cartao_de_credito as (
        select 
            cast(creditcardid as integer) as id_cartao_de_credito
            ,cast(cardtype as string) as tipo_cartao_de_credito

            --,cardnumber
            --,expmonth
            --,expyear
            --,modifieddate
        from {{ source('erp adventure works', 'sales_creditcard') }}
    )

 select *
 from fonte_cartao_de_credito
 