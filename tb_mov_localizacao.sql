create table mov_localizacao(
nome varchar(15),
quantidade decimal(30,2),
valor decimal(30,2),
movimento varchar(10),
localizacao varchar(30),
data_movimento date
)
truncate table mov_localizacao
insert into mov_localizacao(nome,quantidade ,valor,movimento ,localizacao ,data_movimento )
select q.nome_produto,sum(q.quantidade),sum(q.valor*q.quantidade), q.movimento, q.localizacao, q.data_movimento from
(select 
case when a.tipo_movimento in ('ENT','AJE') then 'ENTRADA' else 'SAIDA' end as movimento,
a.quantidade as quantidade,
a.data_movimento,
a.valor_unitario as valor,
b.descricao as nome_produto,
d.descricao as localizacao
from mov_estoque a
inner join produto b on a.cod_prod = b.codigo_do_produto
inner join localizacao d on a.localizacao = d.sigla
) as q
group by q.data_movimento, q.movimento, q.nome_produto, q.localizacao
order by q.data_movimento

select * from mov_localizacao