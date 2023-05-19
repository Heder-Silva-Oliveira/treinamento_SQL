use vendas;

create table pedido(
npedido int not null,
codcli int not null,
data_venda date not null,
forma_pagamento varchar(30)
)

alter table pedido add constraint pk_pedido primary key (npedido);
alter table pedido add constraint fk_clientes foreign key (codcli)
references clientes(codcli);

insert into pedido (npedido, codcli, data_venda, forma_pagamento)
values(4,7,'13-12-2023','Dinheiro');

select * from pedido;
delete pedido where npedido =3;


