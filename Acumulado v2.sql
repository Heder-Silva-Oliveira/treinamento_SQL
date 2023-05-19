create table acumulado(
cod_prod int not null,
tipo_movimentacao varchar(25) not null,
quantidade int not null,
valor decimal(20,2)
);

truncate table acumulado 
insert into acumulado (cod_prod, tipo_movimentacao, quantidade, valor)
select 
	cod_prod,
	tipo_movimento,
	sum(quantidade),
	sum(valor_unitario*quantidade)
from mov_estoque
group by cod_prod, tipo_movimento

select*from acumulado




insert into acumulado_final (cod_prod, tipo_movimentacao, quantidade, valor)
select
cod_prod as codigo_produto,
'ENT' as tipo_movimento,
sum(quantidade) as quantidade,
sum(valor) as valor
from acumulado_v3
where tipo_movimentacao in ('ENT', 'AJE')
group by cod_prod
union
select
cod_prod as codigo_produto,
'SAD' as tipo_movimento,
sum(quantidade) as quantidade,
sum(valor) as valor
from acumulado_v3
where tipo_movimentacao in ('SAD', 'AJS')
group by cod_prod

select * from acumulado_final