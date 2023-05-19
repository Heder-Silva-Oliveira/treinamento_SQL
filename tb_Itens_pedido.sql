create table itens_pedido (
num_ped int not null,
num_item int not null,
cod_prod int not null,
cod_forn int not null,
quat int not null,
valor_uni decimal (5,2) not null 
);

alter table itens_pedido add constraint pk_itens_pedido primary key (num_ped, num_item);

alter table itens_pedido add constraint fk_pedido foreign key (num_ped)
references pedido(npedido);

alter table itens_pedido add constraint fk_produto foreign key (cod_prod)
references produto(codigo_do_produto);

alter table itens_pedido add constraint fk_prod_forn foreign key (cod_prod, cod_forn)
references prod_forn(cod_prod, cod_forn);

select * from pedido
select * from produto
select * from fornecedor
select * from itens_pedido
select * from prod_forn

sp_help itens_pedido
sp_help prod_forn
insert into itens_pedido(num_ped, num_item, cod_prod, cod_forn, quat, valor_uni)
values
(3,6,5,3,11,100.00),
(5,8,6,4,1,125.10),
(11,7,5,3,5,17.10),
(9,2,4,2,8,5.50),
(10,9,3,2,10,3.10);
