create or alter procedure sp_entradapedido
@num_ped int, 
@num_item int, 
@cod_prod int,
@cod_forn int,
@quat int,
@valor_uni decimal(5,2),
@codcli int , 
@data_venda date ,
@forma_pagamento varchar(30) as
if(select count(*) from pedido where npedido = @num_ped) > 0
begin
	print 'Este pedido ja existe'
	return
end
if(select count(*) from prod_forn where cod_prod = @cod_prod and cod_forn = @cod_forn) < 1
begin
	print 'Produto ou fornecedor não existe'
	return
end
if(select count(*) from prod_forn where cod_prod = @cod_prod and cod_forn = @cod_forn) < 1
begin
	print 'Produto ou fornecedor não existe'
	return
end
if @quat = 0 or @valor_uni = 0
begin
	print 'Quantidade ou valor menor que 0'
	return
end
if (select count(*) from estoque where cod_prod = @cod_prod) < 1
begin
	print 'Produto não existe'
	return
end
if (select saldo from estoque where cod_prod = @cod_prod) < @quat
begin 
	print 'Sem estoque'
	return
end

begin tran
insert into pedido (npedido, codcli, data_venda, forma_pagamento)
values(@num_ped, @codcli, @data_venda, @forma_pagamento);

insert into itens_pedido(num_ped, num_item, cod_prod, cod_forn, quat, valor_uni)
values(@num_ped, @num_item, @cod_prod, @cod_forn, @quat, @valor_uni);

update estoque 
set saldo = saldo-@quat, qtd_saida =qtd_saida + @quat where cod_prod = @cod_prod
commit
return
