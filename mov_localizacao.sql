/* Diego e Douglas
Criar tabela a partir da 'itensdopedido'
com npedido e nitem, nomeproduto, nomefornecedor, qtde e valorunitario 

José e Heder
Criar tabela a partir da Mov_Estoque
com data_movimento, tipo_movimento, quantidade, valor_total, nome_produto
e colocar a descrição de cada localizacao. Ex: A1 = "Corredor 3, quadra 4"  */


/* 
		TABELA DE LOCALIZACAO
*/

create table localizacao(
sigla varchar(2) not null,
descricao varchar(30) not null
)

insert into localizacao(sigla, descricao)
values
('A1', 'Corredor UM, quadra 001'),
('A2', 'Corredor UM, quadra 002'),
('A3', 'Corredor UM, quadra 003'),
('B1', 'Corredor DOIS, quadra 001'),
('B2', 'Corredor DOIS, quadra 002'),
('B3', 'Corredor DOIS, quadra 003'),
('C1', 'Corredor TRES, quadra 001'),
('C2', 'Corredor TRES, quadra 002'),
('C3', 'Corredor TRES, quadra 003'),
('D1', 'Corredor QUATRO, quadra 001'),
('D2', 'Corredor QUATRO, quadra 002'),
('D3', 'Corredor QUATRO, quadra 003')

/* 
		TABELA DE MOVIMENTO DE LOCALIZACAO
*/

create table mov_localizacao(
nome varchar(15) not null,
quantidade decimal(30,2) not null,
valor decimal(30,2) not null,
movimento varchar(10) not null,
localizacao varchar(30) not null,
data_movimento date not null
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

/* 

PROVA REAL COM SELECT DAS 3 TABELAS

*/

select * from mov_localizacao;
select * from mov_estoque where cod_prod = 1 and data_movimento = '2023-05-08' and tipo_movimento = 'SAD';
select * from produto;



