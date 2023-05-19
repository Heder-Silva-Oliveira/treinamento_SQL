Use vendas;

create table clientes (
codcli int not null,
nome varchar (50) not null,
endereco varchar(100) not null,
data_de_nascimento date
);

alter table clientes add constraint pk_clientes primary key (codcli);

insert into clientes ( codcli, nome, endereco, data_de_nascimento) 
values ( 01 , 'Heder', 'Londrina', '21-04-1990');
insert into clientes ( codcli, nome, endereco, data_de_nascimento) 
values ( 02 , 'Antonio', 'Londrina', '01-04-2000');
insert into clientes ( codcli, nome, endereco, data_de_nascimento) 
values ( 03 , 'Marcos', 'Londrina', '01-08-1999');
insert into clientes ( codcli, nome, endereco, data_de_nascimento) 
values ( 04 , 'Ana', 'Londrina', '28-12-1995');
insert into clientes ( codcli, nome, endereco, data_de_nascimento) 
values ( 05 , 'Marta', 'Londrina', '18-07-2001');

select * from clientes;

delete clientes where codcli = 1;

