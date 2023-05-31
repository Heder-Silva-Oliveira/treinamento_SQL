
select nome_prod, quantidade, valor, nome_mov, nome_local from produto_dim  a
inner join mov_localizacao_fato b on a.id = b.codido_prod
inner join movimento_dim c on b.cod_mov = c.id
inner join localizacao_dim d on b.cod_loc = d.id
where nome_prod = 'Geladeira'


select nome_prod, quantidade, valor, nome_mov, nome_local from localizacao_dim a
inner join mov_localizacao_fato b on a.id= b.cod_loc
inner join produto_dim c on b.codido_prod = c.id
inner join movimento_dim d on b.cod_mov = d.id
where nome_local like 'Corredor esquerdo%' and nome_mov like 'SAIDA'

select * from mov_localizacao_fato