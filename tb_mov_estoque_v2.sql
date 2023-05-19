use vendas
select * from estoque;
select * from mov_estoque ;
-- Douglas e Jose
--Criar uma tabela de movimento de estoque com:
--1-id auto increment int, 
--2-data_movimento data,
--3-tipo movimento varchar (3) ENT ou SAD ou AJU (ajuste)(check pesquisar),
--4-quantidade int,
--5-valor decimal,
--6-cod_prod, 
--7-localizaçao) 
--drop table mov_estoque 
create table mov_estoque(
id int IDENTITY(1,1) not null,
data_movimento date not null,
tipo_movimento varchar(3) CHECK (tipo_movimento in ('ENT','SAD','AJE','AJS')),
quantidade decimal(30,2) not null,
valor_unitario decimal(30,2) not null,
cod_prod int not null,
localizacao varchar(30) not null
);

insert into mov_estoque (data_movimento,tipo_movimento,quantidade,valor_unitario,cod_prod,localizacao)
values 
('2023-10-12', 'ENT', 3, 199.00, 2,'A3');

-- pk id
alter table mov_estoque add constraint pk_id_mov_estoque primary key (id);

--2 indices por cod_prod e outro por localização
create index idx02_mov_estoque on mov_estoque(cod_prod);
create index idx03_mov_estoque on mov_estoque(localizacao);

--2- criar uma procedure, entrada na tabela de estoque, e registre o movimento 
--nessa tabela de movimento
alter procedure sp_estoque_e_mov
@data_movimento date,
@tipo_movimento varchar(3),
@quantidade decimal(30,2),
@valor_unitario decimal(30,2),
@cod_prod int,
@localizacao varchar(30)

as
-- condição 'Quantidade ou valor unitario nao podem ser igual a Zero'
if @quantidade = 0 or @valor_unitario = 0
begin
	print'Quantidade ou valor unitario igual a Zero'
	return
end
-- Condição produto precisa existir
if (select count(*) from estoque where cod_prod = @cod_prod) < 1 
begin
	print'Produto não existe no estoque'
	return
end
--Condição produto precisa estar disponivel em estoque saldo< quantidade
if (select saldo from estoque where cod_prod = @cod_prod) < @quantidade and @tipo_movimento in ('SAD','AJS')
begin
	print'Sem estoque'
	return
end
--@tipo_movimento ENT ou SAD ou AJU 
begin tran 
--Entrada inserção
if @tipo_movimento in ('ENT', 'AJE')
begin
insert into mov_estoque(data_movimento,tipo_movimento, quantidade, valor_unitario, cod_prod,localizacao)
values (@data_movimento,@tipo_movimento,@quantidade,@valor_unitario,@cod_prod,@localizacao)
update estoque
set saldo = saldo + @quantidade, qtd_entrada = qtd_entrada + @quantidade
where cod_prod = @cod_prod
end


--Saida inserção
if @tipo_movimento in ('SAD','AJS') 
begin
insert into mov_estoque(data_movimento,tipo_movimento, quantidade, valor_unitario, cod_prod,localizacao)
values (@data_movimento,@tipo_movimento,@quantidade,@valor_unitario,@cod_prod,@localizacao)
update estoque
set saldo = saldo - @quantidade, qtd_saida = qtd_saida + @quantidade
where cod_prod = @cod_prod
end

commit 
return;

--EXECUTANDO
execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'ENT',@quantidade = 2,
@valor_unitario = 200, @cod_prod = 2, @localizacao= 'A2'

execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'SAD',@quantidade = 2,
@valor_unitario = 200, @cod_prod = 2, @localizacao= 'A2'

execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'SAD',@quantidade = 2,
@valor_unitario = 200, @cod_prod = 1, @localizacao= 'A1'

execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'ENT',@quantidade = 2,
@valor_unitario = 200, @cod_prod = 1, @localizacao= 'A1'

execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'AJS',@quantidade = 50,
@valor_unitario = 200, @cod_prod = 2, @localizacao= 'A2'

execute sp_estoque_e_mov @data_movimento = '2023-01-05', @tipo_movimento = 'AJE',@quantidade = 50,
@valor_unitario = 300, @cod_prod = 3, @localizacao= 'A3'




--3- Alteração na sp de entrada de pedidos, todas vez que entrar pedido registrar na tabela
--mov de estoque




--Diego e heder
--1- criar uma tabela historico (select into)  (tabela flat - )
--npedido
--nome cliente
--nomeproduto
--quantidade
--valor
--nome fornecedor
--localizao estoque produto
--qtde saida por produto (tabela movimento de estoque)
--qtde entrada por produto (tabela movimento de estoque)

--modelagem de estrela?
--banco de adventure works futuro



update estoque 
set qtd_saida = 10 where id =2;
select * from estoque
delete from estoque where id in ( 12,13,11); 