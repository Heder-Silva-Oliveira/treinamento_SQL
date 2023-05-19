drop table historico_final
select 
a.num_ped as numero_pedido,
d.nome as cliente,
a.cod_prod as codigo_do_produto,
e.descricao as produto,
a.quat as qtd_uni,
ent.quantidade as qtd_tt_entrada,
isnull(format((cast(a.quat  as decimal ) / ent.quantidade),'P2') ,'') as porcent_qtd_ent,
isnull(sad.quantidade, 0) as qtd_tt_saida,
isnull(format((cast(a.quat  as decimal ) / sad.quantidade),'P2'),'') as porcent_qtd_sad,
a.valor_uni as valor_uni,
ent.valor as valor_entrada,
isnull(format((a.valor_uni/ent.valor ),'P'),'') as porcent_vlr_ent,
isnull(sad.valor, 0) as valor_saida,
isnull(format((a.valor_uni /sad.valor),'P'),'') as porcent_vlr_sad,
f.nome as nome_fornecedor,
ent.tipo_movimentacao  as mov_entrada,
isnull(sad.tipo_movimentacao, 'SAD') as mov_saida,
CONVERT (date, GETDATE()) as data_processo
into historico_final
from  itens_pedido a
inner join pedido c on a.num_ped = c.npedido
inner join clientes d on c.codcli = d.codcli
inner join produto e on a.cod_prod = e.codigo_do_produto
inner join fornecedor f on a.cod_forn = f.cod
inner join acumulado_final ent on a.cod_prod = ent.cod_prod and ent.tipo_movimentacao = 'ENT'
left join acumulado_final sad on a.cod_prod = sad.cod_prod and sad.tipo_movimentacao = 'SAD'

