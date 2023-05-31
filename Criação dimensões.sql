create table produto_dim(
id int not null identity(1,1),
nome_prod varchar(30)
);
create table localizacao_dim (
id int not null identity(1,1),
nome_local varchar(30)
);
create table movimento_dim (
id int not null identity(1,1),
nome_mov varchar(30)
);


insert into produto_dim ( nome_prod) 
select
distinct 
nome 
from mov_localizacao

insert into localizacao_dim (nome_local) 
select
distinct localizacao 
from mov_localizacao

insert into movimento_dim (nome_mov) 
select
distinct movimento
from mov_localizacao



select 
case when a.nome = b.nome_prod then b.id end as codido_prod,
a.quantidade as quantidade,
a.valor as valor,
case when a.movimento = c.nome_mov then c.id end as cod_mov,
case when a.localizacao = d.nome_local then d.id end as cod_loc,
a.data_movimento as data_movimento
into mov_localizacao_fato
from mov_localizacao a
inner join produto_dim b on a.nome = b.nome_prod
inner join movimento_dim c on a.movimento = c.nome_mov
inner join localizacao_dim d on a.localizacao = d.nome_local


