create procedure sp_estoque
@localizacao varchar(5),
@cod_prod int,
@saldo decimal(5,2),
@qtd_entrada decimal(5,2),
@qtd_saida decimal(5,2)
as
insert into estoque (localizacao, cod_prod, saldo, qtd_entrada, qtd_saida) values
(@localizacao,@cod_prod,@saldo,@qtd_entrada,@qtd_saida)
return
execute sp_estoque
@localizacao='B1',
@cod_prod=9,
@saldo=10,
@qtd_entrada=0,
@qtd_saida=0;
