/*Criar tabela a partir da Mov_Estoque Ex: A1 = "Corredor 3, quadra 4"Criar tabela a partir da Mov_Estoque
com data_movimento, tipo_movimento, quantidade, valor_total, nome_produto
e colocar a descrição de cada localizacao. Ex: A1 = "Corredor 3, quadra 4"*/
select q.nome_produto,sum(q.quantidade),sum(q.valor*q.quantidade), q.movimento,q.data_movimento,  q.localizacao from
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
select * from historico_final
