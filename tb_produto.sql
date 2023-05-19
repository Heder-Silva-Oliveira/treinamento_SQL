create table produto(
codigo_do_produto int not null, 
descricao varchar(30) not null,
local_no_estoque varchar(30) not null,
classificacao int not null
);

select * from produto;

alter table produto add constraint pk_codigo_do_produto primary key (codigo_do_produto);

insert into produto (codigo_do_produto, descricao, local_no_estoque, classificacao)
values(1,'Ventilador','Corredor A', 1);
