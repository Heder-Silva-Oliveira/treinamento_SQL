from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, lit, udf,monotonically_increasing_id
from pyspark.sql.types import ShortType, BooleanType, StructType, StructField, StringType, IntegerType, LongType, DateType, DecimalType, TimestampType
import re
# Definição do schema
schemaVendas = StructType([
    StructField("NOME", StringType(), True),
    StructField("CNPJ", StringType(), True),
    StructField("EMAIL", StringType(), True),
    StructField("TELEFONE", StringType(), True),
    StructField("NUMERO_NF", LongType(), True),
    StructField("DATA_EMISSAO", DateType(), True),
    StructField("VALOR_NET", DecimalType(16,2), True),
    StructField("VALOR_TRIBUTO", DecimalType(16,2), True),
    StructField("VALOR_TOTAL", DecimalType(16,2), True),
    StructField("NOME_ITEM", StringType(), True),
    StructField("QTD_ITEM", IntegerType(), True),
    StructField("CONDICAO_PAGAMENTO", StringType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("NUM_ENDERECO", IntegerType(), True),
    StructField("COMPLEMENTO", StringType(), True),
    StructField("TIPO_ENDERECO", StringType(), True),
    StructField("DATA_PROCESSAMENTO", TimestampType(), True)
])
schemaValidaVenda = StructType([
    StructField("DATA_PROCESSAMENTO", TimestampType(), nullable=True),
    StructField("DATA_EMISSAO", DateType(), nullable=True),
    StructField("NUMERO_NF", IntegerType(), nullable=True),
])
schemaEnderecoCliente = StructType([
    StructField("ID_CLIENTE",IntegerType(), False),
    StructField("ID_TIPO_ENDERECO", IntegerType(), False),
    StructField("CEP", StringType(), False),
    StructField("NUMERO", IntegerType(), False),
    StructField("COMPLEMENTO", StringType(), True)
])
schemaCliente = StructType([
    StructField("NOME", StringType(), False),
    StructField("CNPJ", StringType(), False),
    StructField("EMAIL", StringType(), False),
    StructField("TELEFONE", StringType(), False)
])
schemaTipoEndereco = StructType([
    StructField("ID_TIPO_ENDERECO",IntegerType(), False),
    StructField("DESCRICAO", StringType(), False),
    StructField("SIGLA", StringType(), False)
])
schemaNotaFiscalSaida = StructType([
    StructField("ID_CLIENTE", IntegerType(), False),
    StructField("ID_CONDICAO", IntegerType(), False),
    StructField("NUMERO_NF", IntegerType(), False),
    StructField("DATA_EMISSAO", DateType(), False),
    StructField("VALOR_NET", DecimalType(16,2), False),
    StructField("VALOR_TRIBUTO", DecimalType(16,2), False),
    StructField("VALOR_TOTAL", DecimalType(16,2), False),
    StructField("NOME_ITEM", StringType(), False),
    StructField("QTD_ITEM", IntegerType(), False)
])
schemaCondicaoPagamento = StructType([
    StructField("ID_CONDICAO",IntegerType(), False),
    StructField("DESCRICAO", StringType(), False),
    StructField("QTD_PARCELAS", IntegerType(), False),
    StructField("ENTRADA", ShortType(), False)
])
# Criação da SparkSession
spark = SparkSession.builder.appName("Tratar").getOrCreate()

# Leitura dos dados
cep_data = spark.read.option("header", True).csv("datat/CEP_BR.csv")
validacao_vendas = spark.read.options(header=True).schema(schemaValidaVenda).csv("datat/validacao_vendas.csv")
vendas = spark.read.options(header=True).schema(schemaVendas).csv('datat/vendas.csv')
data_cliente = spark.read.options(header=True).schema(schemaCliente).csv('datat/clientes.csv')
data_endereco_cliente = spark.read.options(header=True).schema(schemaEnderecoCliente).csv('datat/enderecos_clientes.csv')
tipo_endereco = spark.read.options(header=True).schema(schemaTipoEndereco).csv('datat/tipo_endereco.csv')
nf_saida_final = spark.read.options(header=True).schema(schemaNotaFiscalSaida).csv('datat/nfs.csv')
condicao_pagamento  = spark.read.options(header=True).schema(schemaCondicaoPagamento).csv('datat/condicao_pagamento.csv')
# Definição da função para validar CNPJ
def cnpj_valido_func(cnpj):
    #cnpj = re.sub(r'[^0-9]', '', cnpj), isso esta errado porque se na nota estiver letra ele retira a letra para validar, mas nanota continua a as letras
    cnpj = str(cnpj)
    cnpj = re.sub(r'[.,"\'-]', '', cnpj)

    if len(cnpj) != 14:
        return False
    
    total = 0 
    resto = 0 
    digito_verificador_1 = 0
    digito_verificador_2 = 0
    multiplicadores1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    multiplicadores2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    for i in range(0, 12, 1):
        total += int(cnpj[i]) * int(multiplicadores1[i])
    resto = total % 11
    
    if resto < 2:
        digito_verificador_1 = 0
    else:
        digito_verificador_1 = 11 - resto

    total = 0
    resto = 0

    for i in range(0, 13, 1):
        total += int(cnpj[i]) * int(multiplicadores2[i])

    resto = total % 11
    if resto < 2:
        digito_verificador_2 = 0 
    else:
        digito_verificador_2 = 11 - resto

    return cnpj[-2:] == str(digito_verificador_1) + str(digito_verificador_2)
cnpj_valido_udf = udf(cnpj_valido_func, BooleanType())

# Realiza o processamento dos dados
vendas_lancar = vendas.join(validacao_vendas, on=['NUMERO_NF', 'DATA_PROCESSAMENTO'], how='left_anti')

cep_data_list = cep_data.withColumn('CEP_VALIDO', col('CEP')).select("CEP_VALIDO")

vendas_lancar1 = vendas_lancar.join(cep_data_list, vendas_lancar["CEP"] == cep_data_list['CEP_VALIDO'], 'left')

vendas_lancar1 = vendas_lancar1.withColumn('CEP_VALIDO', when(col('CEP_VALIDO').isNull(), 'INVALIDO').otherwise("VALIDO"))

# Aplica a validação do CNPJ
vendas_lancar1 = vendas_lancar1.withColumn("CNPJ_VALIDO", cnpj_valido_udf("CNPJ"))

# Realiza as validações e tratamentos
venda_tratada = vendas_lancar1.withColumn("NOME", upper(col("NOME")))\
    .withColumn("MOTIVO",\
    when(col("NOME").isNull(), "NOME_NULO")\
    .when(col("CNPJ_VALIDO") == False, "CNPJ_INVALIDO")\
    .when(col("NUMERO_NF").isNull(), "NUMERO_NF_NULO")\
    .when(col("DATA_EMISSAO").isNull(), "DATA_EMISSAO_NULO")\
    .when(col("VALOR_NET").isNull(), "VALOR_NET_NULO")\
    .when(col("VALOR_TRIBUTO").isNull(), "VALOR_NET_NULO")\
    .when(col("VALOR_TOTAL").isNull(), "VALOR_TOTAL")\
    .when(col("NOME_ITEM").isNull(), "NOME_ITEM_NULO")\
    .when(col("CONDICAO_PAGAMENTO").isNull(), "CONDICAO_PAGAMENTO_NULO")\
    .when(col("CEP_VALIDO") == 'INVALIDO', "CEP_INVALIDO")\
    .when(col("TIPO_ENDERECO").isNull(), "TIPO_ENDERECO_NULO")  
    .otherwise("OK"))\
    .withColumn('NUM_ENDERECO', when(col("NUM_ENDERECO").isNull(), lit(0)).otherwise(col("NUM_ENDERECO")))\
    .withColumn('COMPLEMENTO', when(col("COMPLEMENTO").isNull(), "N/A").otherwise(col("COMPLEMENTO")))\
    .dropDuplicates()

# Filtra os registros aptos e rejeitados
vendas_aptas = venda_tratada.filter(col("MOTIVO") == "OK").drop("CNPJ_VALIDO", "CEP_VALIDO", "MOTIVO")
vendas_rejeitadas = venda_tratada.filter(col("MOTIVO") != "OK").drop("CNPJ_VALIDO", "CEP_VALIDO")


validar_vendas = vendas_aptas.select('DATA_PROCESSAMENTO','NUMERO_NF', 'DATA_EMISSAO')



#V E N D A S ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

vendas_validado_final = (vendas_aptas
    .withColumn("CONDICAO_PAGAMENTO", when(col("CONDICAO_PAGAMENTO").substr(2, 4).like("%ntra%"), col("CONDICAO_PAGAMENTO"))
        .otherwise(when(col("CONDICAO_PAGAMENTO").like("%90 dias") | col("CONDICAO_PAGAMENTO").like("%noventa dias"), "30/60/90 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%60 dias"), "30/60 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%vista"), "A vista")
            .otherwise(col("CONDICAO_PAGAMENTO")))
    ))

vendas_validado_final = vendas_validado_final\
    .withColumn("NUMERO_NF", col("NUMERO_NF").cast("long"))\
    .withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date"))\
    .withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(16,2)"))\
    .withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(16,2)"))\
    .withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(16,2)"))\
    .withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer"))\
    .withColumn("CEP", col("CEP").cast("integer"))\
    .withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer"))\
    .withColumn("DATA_PROCESSAMENTO", col("DATA_PROCESSAMENTO").cast("timestamp")).drop('CNPJ_VALIDO')
   
#vendas_validado_final = spark.createDataFrame(vendas_validado_final.collect(), schema = schemaVendas)     

#C L I E N T E ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

novos_cliente = vendas_validado_final.select("NOME", col("CNPJ").cast("long"), "EMAIL", "TELEFONE")

# Atualizar cliente
update_cliente = novos_cliente.join(
    data_cliente,
    "CNPJ",
    "inner"
).filter(
    (data_cliente["NOME"] != novos_cliente["NOME"]) |
    (data_cliente["EMAIL"] != novos_cliente["EMAIL"]) |
    (data_cliente["TELEFONE"] != novos_cliente["TELEFONE"])
)

# Novo cliente
inserir_clientes = novos_cliente.join(
    data_cliente,
    "CNPJ",

    "left_anti"
)
inserir_clientes = inserir_clientes.coalesce(1).withColumn('ID_CLIENTE', monotonically_increasing_id()+1)

#E N D E R E C O   C L I E N T E ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
tipo_endereco_temp = tipo_endereco.withColumnRenamed("DESCRICAO", "TIPO_ENDERECO")\
                                .withColumn("TIPO_ENDERECO", upper(col("TIPO_ENDERECO")))
#Clientes com ID
ids_clientes=inserir_clientes.select("ID_CLIENTE","CNPJ")
# Exiba os resultados
join_completo = (vendas_validado_final
                 .join(novos_cliente, "CNPJ", "left")
                 .join(tipo_endereco_temp, "TIPO_ENDERECO", "left")
                 .join(ids_clientes, "CNPJ", "inner"))

# Use select para selecionar apenas as colunas necessárias:
dados_endereco_com_ids = (join_completo
                          .select("ID_CLIENTE", "ID_TIPO_ENDERECO", "CEP", "NUM_ENDERECO", "COMPLEMENTO"))

# Atualizar endereço
update_endereco_cliente = (dados_endereco_com_ids.join(
    data_endereco_cliente,
    (dados_endereco_com_ids["ID_CLIENTE"] == data_endereco_cliente["ID_CLIENTE"]) &
    (dados_endereco_com_ids["CEP"] == data_endereco_cliente["CEP"]),
    "inner"
).filter(
    (data_endereco_cliente["ID_TIPO_ENDERECO"] != dados_endereco_com_ids["ID_TIPO_ENDERECO"]) |
    (data_endereco_cliente["NUMERO"] != dados_endereco_com_ids["NUM_ENDERECO"]) |
    (data_endereco_cliente["COMPLEMENTO"] != dados_endereco_com_ids["COMPLEMENTO"])
).drop(data_endereco_cliente["ID_TIPO_ENDERECO"],
        data_endereco_cliente["NUMERO"],
        data_endereco_cliente["COMPLEMENTO"]))

# Novo endereço
novo_endereco_cliente = (dados_endereco_com_ids.join(
    data_endereco_cliente,
    (dados_endereco_com_ids["ID_CLIENTE"] == data_endereco_cliente["ID_CLIENTE"]) &
    (dados_endereco_com_ids["CEP"] == data_endereco_cliente["CEP"]),
    "left_anti"
))

insert_endereco_cliente = (novo_endereco_cliente.coalesce(1)
                            .withColumn('ID_ENDERECO_CLIENTE', monotonically_increasing_id()+1))

condicao_pagamento_temp = condicao_pagamento.withColumnRenamed("DESCRICAO", "CONDICAO_PAGAMENTO")

nf_saida = (vendas_validado_final.join(ids_clientes, "CNPJ", "left")
            .join(condicao_pagamento_temp, "CONDICAO_PAGAMENTO", 'left')
            .withColumnRenamed("ID", "ID_CONDICAO_PAGAMENTO")
            .select("ID_CLIENTE","ID_CONDICAO","NUMERO_NF","DATA_EMISSAO","VALOR_NET","VALOR_TRIBUTO","VALOR_TOTAL","NOME_ITEM","QTD_ITEM"))

nf_saida = nf_saida.withColumn("ID_CLIENTE", col("ID_CLIENTE").cast("integer"))\
    .withColumn("ID_CONDICAO", col("ID_CONDICAO").cast("integer"))

# Insert nota fiscal
nf_saida_final = nf_saida.coalesce(1).withColumn('ID_NF_SAIDA', monotonically_increasing_id()+1)
print("NOTA FISCAL")
nf_saida_final.show()
'''
# Insert validação
nf_saida_final = nf_saida.coalesce(1).withColumn('ID_NF_SAIDA', monotonically_increasing_id()+1)

# Insert rejeitados
rejeitadas = vendas_rejeitadas.coalesce(1).withColumn('id_vendas_rejeitadas', monotonically_increasing_id()+1)

print("NOTA rejeitadas")
rejeitadas.show()'''
spark.stop()
