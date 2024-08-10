/*Carlos mencionou que as vendas brutas no ano de 2011 foram de $12.646.112,16. */

with
    vendas_brutas_2011 as (
        select (sum(total_bruto)) as total_bruto_vendido
        from {{ ref('fato_pedidos') }}
        where data_pedido between '2011-01-01' and '2011-12-31'
    )

select total_bruto_vendido
from vendas_brutas_2011
where total_bruto_vendido not between 12646111.1607 and 12646113.1607
