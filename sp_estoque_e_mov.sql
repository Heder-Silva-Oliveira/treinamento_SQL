create or alter procedure sp_estoque_e_mov
@data_movimento date,
@tipo_movimento varchar(3),
@quantidade decimal(30,2),
@valor_unitario decimal(30,2),
@cod_prod int,
@localizacao varchar(30)

as

if @quantidade = 0 or @valor_unitario = 0
begin
	print'Quantidade ou valor unitario igual a Zero'
	return
end

if (select count(*) from estoque where cod_prod = @cod_prod) < 1 
begin
	print'Produto n?o existe no estoque'
	return
end

if (select saldo from estoque where cod_prod = @cod_prod) < @quantidade and @tipo_movimento in ('SAD','AJS')
begin
	print'Sem estoque'
	return
end

begin tran 

if @tipo_movimento in ('ENT', 'AJE')
begin
insert into mov_estoque(data_movimento,tipo_movimento, quantidade, valor_unitario, cod_prod,localizacao)
values (@data_movimento,@tipo_movimento,@quantidade,@valor_unitario,@cod_prod,@localizacao)
update estoque
set saldo = saldo + @quantidade, qtd_entrada = qtd_entrada + @quantidade
where cod_prod = @cod_prod
end


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