create table prod_forn(
cod_prod int not null,
cod_forn int not null,
data_cad date not null,
nota int not null
);
alter table prod_forn add constraint fk_prod foreign key (cod_prod)
references produto(codigo_do_produto);
alter table prod_forn add constraint fk_forn foreign key (cod_forn)
references fornecedor(cod);

alter table prod_forn add constraint pk_cod_id primary key (cod_prod, cod_forn);

create table fornecedor(
cod int not null,
nome varchar(30) not null,
ende varchar(50) not null,
contato varchar(15) not null
);
alter table fornecedor add constraint pk_cod primary key (cod);