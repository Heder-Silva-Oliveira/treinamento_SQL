create table mov_estoque(
id int IDENTITY(1,1) not null,
data_movimento date not null,
tipo_movimento varchar(3) CHECK (tipo_movimento in ('ENT','SAD','AJE','AJS')),
quantidade decimal(30,2) not null,
valor_unitario decimal(30,2) not null,
cod_prod int not null,
localizacao varchar(30) not null
);



alter table mov_estoque add constraint pk_id_mov_estoque primary key (id);

create index idx02_mov_estoque on mov_estoque(cod_prod);
create index idx03_mov_estoque on mov_estoque(localizacao);
