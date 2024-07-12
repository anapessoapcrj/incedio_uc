# Databricks notebook source
# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC INTRODUÇÃO: APRESENTAÇÃO DO PROBLEMA E A ESCOLHA DO DATASET PARA O MVP
# MAGIC
# MAGIC
# MAGIC Hoje vivemos em um cenário, bastante preocupante, onde não e mais possível tolerar ações predatórias contra o meio ambiente, sejam estas os desmatamentos; os incêndios contra Vegetações; emissões de poluentes na atmosfera, solos, rios e mares.  Não importa. Todas geram efeitos adversos e nos cobrarão faturas cada vez mais alta em prazos cada vez mais curtos. As consequências das ações predatórias ambientais não se restringem apenas ao declínio ou mesmo a extinção de espécies da fauna e flora, mas se estende sobre nós, sociedade, expondo-nos a maiores riscos e piorando a nossa qualidade de vida. Com esta reflexão em mente, busquei por um conjunto de dados que tratasse dessas mazelas que atingem a natureza e a sociedade simultaneamente.
# MAGIC  
# MAGIC
# MAGIC O Instituto Chico Mendes de Conservação da Biodiversidade, ICMBio, disponibiliza no Portal de Dados do Governo Federal, Gov.BR, dados que se alinham ao desejo manifestado acima e, assim, foi selecionado o arquivo intitulado de [Áreas queimadas em unidades de conservação federais](https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs) para ser utilizado neste MVP.  Outro dataset, localizado no mesmo ambiente, a ser utilizado para dar mais qualidade aos dados é o [Atributos das Unidades de Conservacao Federais]( (https://dados.gov.br/dados/conjuntos-dados/atributos-das-unidades-de-conservacao-federais) .  Mais adiante será aprofundado sobre os conjuntos de dados utilizados aqui em Metadado e  Catálago, mas oportunamente será deixado este [link](https://www.gov.br/icmbio/pt-br/acesso-a-informacao/dados-abertos) que disponibiliza de forma resumida informações sobre as tabelas disponibilizadas.
# MAGIC
# MAGIC Após a estruturação dos dados provenientes destes arquivos buscar-se-á as respostas para as seguintes questões: Quais Unidades de Conservação tiveram as maiores áreas queimadas atingidas por incêndios? Entre o intervalo de tempo escolhido, 2018-2023, quais UCs sofreram com as maiores frequências de queimadas? Considerando o Bioma predominante de cada UCs, quais foram as mais afetadas por incêndio? E por fim, tem se a ideia de unir as tabelas de incêndio, que na originalmente esta separado por ano em planilhas do arquivo excel, para visualizar em gráfico de linha, como foi a evolução dos incêndios nas Unidades de Conservação.
# MAGIC
# MAGIC Obs.: os arquivos originais estão formato em excel, sendo necessário a conversão para CSV para o UPLOAD dentro da nuvem do Databricks Community, a versão gratuita para usuários. Para tornar este processo mais ágil, a conversão foi feita no Google Colab onde foi realizada antes uma formatação nos valores dos atributos, transformando letras minúsculas em maiúsculas; retirando os caracteres especiais e acentuações e, no caso, dos títulos das colunas também retirando os espaços em branco, substituindo-os por underline (_). Toda essas ações foram feitas visado evitar problemas na conversão e no carregamento dos arquivos CSV dentro do Databricks. Para verificar como se deu este etapa inicial do trabalho, [ir no Google Colab](https://colab.research.google.com/drive/10mFX7G46NC4IOu58o_YTPW3TM2xAvgrE?authuser=1) .
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1 - UPLOAD dos arquivos CSV para o Unity Catalog do Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC O início da pipeline de dados dentro do Databricks é a partir do UPLOAD dos arquivos csv para este notebook. Como são vários arquivos, foi criado códigos para iterar e agilizar esta etapa. Para isto, foi criado objetos que carregam e salvam dentro de um diretório do Unity Catalog do Databricks. Na mesma linha de código abaixo, foram criadas duas listas, Uma com as urls dos arquivos convertidos em CSV, hospedados no [GitHub](https://raw.githubusercontent.com/anapessoapcrj/incedio_uc) e a outra com os nomes dos arquivos csv. Ambas as listas, além dos referidos objetos, são utilizados na iteração, sendo possível visualizar na execução deste código, onde e como os arquivos CSV foram salvos.

# COMMAND ----------



catalog = "incedio"
schema = "dados_uc"
volume = "incedio_historico"
path_volume = f"/Volumes/{catalog}/{schema}/{volume}"

# Lista dos URLs dos arquivos CSV
urls = [
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/dadosgerais_uc.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2023.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2022.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2021.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2020.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2019.csv",
    "https://raw.githubusercontent.com/anapessoapcrj/incedio_uc/main/area_atingida_2018.csv"
]

# Lista dos nomes dos arquivos CSV
file_names = [
    "dadosgerais_uc.csv",
    "area_atingida_2023.csv",
    "area_atingida_2022.csv",
    "area_atingida_2021.csv",
    "area_atingida_2020.csv",
    "area_atingida_2019.csv",
    "area_atingida_2018.csv"
]

# Loop para baixar cada arquivo CSV e ler como tabela
for url, file_name in zip(urls, file_names):
    # Caminho completo do arquivo no Databricks
    file_path = f"{path_volume}/{file_name}"
    
    # Baixar o arquivo CSV usando dbutils.fs
    dbutils.fs.cp(url, file_path, recurse=False)
    print(f"Arquivo {file_name} baixado para: {file_path}")


# Exemplo de consulta para verificar as tabelas registradas
spark.sql("SHOW TABLES").show()


# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Agora os arquivos serão visualizados como DataFrames atraves do pyspark. Para isto foi criada sessão spark para carregar os csvs em dataframes. Para usar novamente o iterador, desta vez para enxergar como ficará a estrutura de cada arquivo em Dataframe, foi criado objeto que é a lista das tabelas (arquivos_csv) e outros objetos que são opções usadas para o carregamento CSV para DataFrame. A opção FAILFAST, por exemplo, foi utilizada para interromper este processo caso o CSV tenha alguma falha importante que provoque a perda de dados ou deformação na estruturação das tabelas E de fato, o arquivo csv dadosgerais_uc estava com problema, a mensagem de erro indicou onde estava o problema. Feita a correção, no Github, o arquivo foi transferido para Databricks sem problemas. Apos a execução do código abaixo, verificou-se que o tipo de dados de cada coluna se comportou conforme esperado, inclusive as colunas com informações sobre data (foi utilizado encoding UTF-8 para evitar que a data que estava em formato brasileiro se convertesse em string).   

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

# Cria uma sessão Spark
spark = SparkSession.builder \
    .appName("Carregamento de CSVs em DataFrames") \
    .getOrCreate()

# Lista de caminhos dos arquivos CSV ou nomes de tabelas
lista_tabelas = [
    "/Volumes/incedio/dados_uc/incedio_historico/dadosgerais_uc.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2023.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2022.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2021.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2020.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2019.csv",
    "/Volumes/incedio/dados_uc/incedio_historico/area_atingida_2018.csv"
]

# Configurações CSV comuns
file_type = "csv"
infer_schema = "true" #infere a estrutura da tabela 
first_row_is_header = "true"
modo = "FAILFAST" #evita que caso arquivo csv tenha alguma falha ocorra o carregamento dados de forma errônea 
delimiter = ","
encoding = "utf-8"

# Lista para armazenar os DataFrames carregados
dataframes = []

# Iterar sobre cada tabela na lista e imprimir o esquema
for tabela in lista_tabelas:
    # Carregar o arquivo CSV para DataFrame
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("mode", modo) \
      .option("sep", delimiter) \
      .option("encoding", encoding) \
      .load(tabela)
    
    # Adicionar o DataFrame à lista de DataFrames
    dataframes.append(df)

    # Imprimir o esquema do DataFrame
    print(f"Esquema da tabela {tabela}:")
    df.printSchema()
    print("----------------------------------")

#objetos para cada dataframe
dados_uc = dataframes[0]
incendio_2023_df = dataframes[1]
incendio_2022_df = dataframes[2]
incendio_2021_df = dataframes[3]
incendio_2020_df = dataframes[4]
incendio_2019_df = dataframes[5]
incendio_2018_df = dataframes[6]

# Aqui você pode realizar operações adicionais com cada DataFrame, se necessário


# COMMAND ----------

# MAGIC %md
# MAGIC 2 - Transformação dos Dados

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Observando como ficou o nome de algumas colunas, vê se a necessidade de alguma padronização ou correção nos nomes das colunas (ainda que isto já tenha feito no Gooogle Colab), especialmente na tabelas relacionadas a incêndios.   

# COMMAND ----------

## Padronização/correção no nome das colunas

incendio_2023_df = incendio_2023_df.withColumnRenamed('Perc_AAF_UC', 'Perc_UC')
incendio_2022_df = incendio_2022_df.withColumnRenamed('Perc_AAF_UC', 'Perc_UC')
incendio_2021_df = incendio_2021_df.withColumnRenamed('`Perc_UC', 'Perc_UC')
incendio_2023_df = incendio_2023_df.withColumnRenamed('Area_total', 'Area_queimada')
incendio_2022_df = incendio_2022_df.withColumnRenamed('Area_total', 'Area_queimada')
incendio_2021_df = incendio_2021_df.withColumnRenamed('_c0', 'ind')


# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Area_total foi substituída por Area_queimada nas tabelas incendio_2023_df e incendio_2022_df para ficar congruente com as tabelas dos anos anteriores. Area_total significa as áreas queimadas menos as Áreas de prevenção de incêndios florestais. Também foi substituída Perc_AAF_UC por Perc_UC por entender que o significado é o mesmo, ou seja, percentual de área atingida por incêndio em relação à área da UC

# COMMAND ----------

incendio_2023_df.show(n=0)

# COMMAND ----------

incendio_2022_df.show(n=0)

# COMMAND ----------

#Verificando o dataframe incendio_2021_df que teve mais colunas renomeadas

incendio_2021_df.show(n=0)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Também será excluída a coluna id de dados_uc, pois nela já existem três chaves primarias: CNUC, TIPO_NOME e NOME_UC. O primeiro é o Código da Unidade de Conservação, o segundo a Unidade de Conservação com a Categoria abreviada ou com iniciais e a última e Unidade de Conservação escrita por extenso.

# COMMAND ----------

dados_uc = dados_uc.drop('id')

# COMMAND ----------

dados_uc.show(n=0)

# COMMAND ----------

# MAGIC %md
# MAGIC 2.1 - Verificação de problemas nas inconsistência dos dados

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Os datasets escolhidos para este MVP, apesar de tabelas, não estão estruturada para ser um banco de dados relacional. Portanto, não podem ser utilizados de imediato para consultas sem antes ter um tratamento corretivo e adaptações.  Este fato é mais perceptível nas tabelas que correspondem às ocorrências de incêndios, onde a escritas dos nomes das Unidades de Conservação ocorre de maneira livre, sem uma preocupação em preservar integridade referencial, sem seguir necessariamente um padrão. Tampouco não têm o CNUC, Código Nacional da Unidade de Conservação, condição ideal para união entre tabelas com colunas com atributos equivalentes.  Posto isto, a seguir teremos consultas para identificar os casos onde não houveram intersecções das Unidades de Conservações registradas nas tabelas de ocorrências de incêndio com a tabela de Atributos das Unidades de Conservação. Ora, as colunas UCs presentes nas tabelas de ocorrências de incêndios deveriam ter 100% de correspondência com a coluna TIPO_NOME da tabela dados_uc, pois este dataset tem o registro atualizado de todas as UCs. No entanto tal condição não se satisfaz, pelo divergência da escrita no nome da Unidades de Conservação, por esta razão é necessário a correção e ajuste. Após a localização dos problemas, os dataframes serão salvos em formato de Tabela Delta onde serão realizados as correções e atualizações.

# COMMAND ----------

##Gerando visualizacao temporaria dos dataframes para realizar a consulta pelo spark.sql

dados_uc.createOrReplaceTempView("dados_uc_temp")
incendio_2023_df.createOrReplaceTempView("incendio_2023_temp")
incendio_2022_df.createOrReplaceTempView("incendio_2022_temp")
incendio_2021_df.createOrReplaceTempView("incendio_2021_temp")
incendio_2020_df.createOrReplaceTempView("incendio_2020_temp")
incendio_2019_df.createOrReplaceTempView("incendio_2019_temp")
incendio_2018_df.createOrReplaceTempView("incendio_2018_temp")

# COMMAND ----------

# Criando as query para identificar os casos de nao interseccoes inadequadas.
# As consultas onde o problema não existir devera retorna com reposta vazia.  


query1 = """
    SELECT UC
    FROM incendio_2023_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""

query2 = """
    SELECT UC
    FROM incendio_2022_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""

query3 = """
    SELECT UC
    FROM incendio_2021_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""
query4 = """
    SELECT incendio_2020_temp.*
    FROM incendio_2020_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""
query5 = """
    SELECT UC
    FROM incendio_2019_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""
query6 = """
    SELECT UC
    FROM incendio_2018_temp
    WHERE UC NOT IN
            (SELECT TIPO_NOME 
            FROM dados_uc_temp);
"""

# COMMAND ----------

# Executar a consulta SQL usando spark.sql

resultado_join_INC23_DGER = spark.sql(query1)
resultado_join_INC22_DGER = spark.sql(query2)
resultado_join_INC21_DGER = spark.sql(query3)
resultado_join_INC20_DGER = spark.sql(query4)
resultado_join_INC19_DGER = spark.sql(query5)
resultado_join_INC18_DGER = spark.sql(query6)

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o resultado para verificar onde serao necessária as correções.

# COMMAND ----------

display(resultado_join_INC23_DGER)

# COMMAND ----------

display(resultado_join_INC22_DGER)

# COMMAND ----------

display(resultado_join_INC21_DGER)

# COMMAND ----------

display(resultado_join_INC20_DGER)

# COMMAND ----------

# MAGIC %md
# MAGIC No caso da base de incêndio de 2020 foi necessário verificar todas as colunas, pois não existe APA da Bacia do Rio Parnaíba do Sul. Parece ser um erro de digitação, e ser a APA Bacia Paraíba do Sul. Porém se este for o caso, o Bioma está errado. Não é Cerrado, e sim Mata Atlântica. Será consultada pela palavra PARNAIBA em dados_uc para ver se a área informada sobre APA Bacia do Paraíba do Sul é equivalente ou aproximada. Na tabela de incêndios 2020 o valor informado é 292599.9242 ha. Esta pesquisa será feita mais adiante em conjunto com nomes de UCs não encontrados.

# COMMAND ----------

display(resultado_join_INC19_DGER)

# COMMAND ----------

display(resultado_join_INC18_DGER)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC É possível verificar que a APA DA BACIA PARAIBA DO SUL, PARNA DA SERRA DOS ORGAOS, PARNA DAS SEMPRE VIVAS E REBIO DAS NASCENTES DA SERRA DO CACHIMBO não foram encontrado na tabela Dados Gerais UC. Vamos pesquisar agora em Dados_Gerais_UC como está escrito estas UCs para substituir a digitação de maneira igual a Dados_Gerais_UC para que seja possível a perfeita junção entre as tabelas de incêndio com a de atributos.

# COMMAND ----------

query7 = """
    SELECT TIPO_NOME, NOME_UC, Area
    FROM dados_uc_temp
    WHERE NOME_UC LIKE '%CACHIMBO%' OR NOME_UC LIKE '%SEMPRE%' OR NOME_UC LIKE '%ORGAOS%' OR NOME_UC LIKE '%ORGAOS%' OR NOME_UC LIKE '%PARAIBA%' OR NOME_UC LIKE '%PARNAIBA%'
"""

# COMMAND ----------

pesquisa_nomes_uc = spark.sql(query7)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC A suspeita de erro na digitação em APA da Bacia do Rio Parnaíba do Sul ao inves de APA Bacia da Paraíba do Sul foi confirmada, pois o área da UC informada na tabela de incêndio "bate" com a informada na tabela, 292599.9242 ha, com Atributos das Unidades de Conservação, conforme é possível ver em tabela abaixo. 

# COMMAND ----------

display(pesquisa_nomes_uc)

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Agora que foi finalizado a verificação dos problemas, os dataframes serão salvos para formato de tabela delta.  As correções e ajustes necessários serão feito na sequência. A forma de salvar deste arquivos foram definidos com o modo de sobreescrever tabelas e seus esquemas, assim é possivel reexecutar as linhas de código sem problema interrupção.

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

dados_uc.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_atualizado_jun_2024")

# COMMAND ----------

incendio_2023_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2023_incendio")

# COMMAND ----------

incendio_2022_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2022_incendio")

# COMMAND ----------

incendio_2021_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2021_incendio")

# COMMAND ----------

incendio_2020_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2020_incendio")

# COMMAND ----------

incendio_2019_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2019_incendio")

# COMMAND ----------

incendio_2018_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/FileStore/tables/uc_2018_incendio")

# COMMAND ----------

# MAGIC %md
# MAGIC Correção da digitação da Unidades de Conservação e outros.
# MAGIC   
# MAGIC Será criado uma lista com o caminho onde estão as tabelas Delta e depois em cima desta lista iterar as correções.

# COMMAND ----------

# Lista de caminhos das tabelas Delta Lake
caminhos_tabelas_delta = ["/FileStore/tables/uc_2023_incendio", "/FileStore/tables/uc_2022_incendio", "/FileStore/tables/uc_2021_incendio", "/FileStore/tables/uc_2020_incendio", "/FileStore/tables/uc_2019_incendio","/FileStore/tables/uc_2018_incendio",]

# Carregar todas as tabelas Delta Lake
delta_tables = [DeltaTable.forPath(spark, caminho) for caminho in caminhos_tabelas_delta]

# COMMAND ----------

# Iterar sobre todas as tabelas Delta Lake e executar a atualização
for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'APA BACIA DO PARAIBA DO SUL'",
        set={"UC": "'APA BACIA PARAIBA DO SUL'"}
    )

# COMMAND ----------

for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'APA DA BACIA PARAIBA DO SUL'",
        set={"UC": "'APA BACIA PARAIBA DO SUL'"}
    )

# COMMAND ----------

for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'APA DA BACIA DO RIO PARNAIBA DO SUL'",
        set={"UC": "'APA BACIA PARAIBA DO SUL'"}
    )

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Nem todas as tabelas com registros de incêndio têm a coluna Bioma, não sendo possível iterar a sua correção. Neste caso, esta ação sera feita pontualmente na tabela de incêndios em 2020 para corrigir o Bioma da APA Bacia Paraíba do Sul. O ajuste será substituir CERRADO por MATA ATLANTICA.

# COMMAND ----------

caminhos_tabelas_delta2 = "/FileStore/tables/uc_2020_incendio"

delta_table2 = DeltaTable.forPath(spark, caminhos_tabelas_delta2)

delta_table2.update(
        condition="UC == 'APA BACIA PARAIBA DO SUL'",
        set={"Bioma": "'MATA ATLANTICA'"}
)

# COMMAND ----------

for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'REBIO DAS NASCENTES DA SERRA DO CACHIMBO'",
        set={"UC": "'REBIO NASCENTES DA SERRA DO CACHIMBO'"}
    )

# COMMAND ----------

for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'PARNA DAS SEMPRE VIVAS'",
        set={"UC": "'PARNA DAS SEMPRE-VIVAS'"}
    )

# COMMAND ----------

for delta_tab in delta_tables:
    delta_tab.update(
        condition="UC = 'PARNA DA SERRA DOS ORGAOS '",
        set={"UC": "'PARNA DA SERRA DOS ORGAOS'"}
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 2.2 - Terminado a correção dos atributos das tabelas, daremos inicio a processo de construção de Metadados. Etapa fundamental para a organização e governança dos dados.  

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Para realização desta etapa foi necessário registrar as tabelas deltas no catálogo Spark. Ao fazer isto, é criado um catálogo onde possível inserir comentários que descrevem as características de cada coluna. Esta ação fez-se pelo uso combinado dos comandos ALTER TABLE e CHANGE COLUMN pelo pyspark, além do comando CREATE TABLE e USING delta LOCATION para apontar a tabela delta de referência para criação do catálogo. Outro acréscimo foi sobre informações gerais de cada tabela: como a origem dos dados; quem é o controlador e quem o mantenedor; o período em que este dado é atualizado e quando foi a última atualização; quem modificou o dado e em que período ocorreu. Para esta operação, foi usado a linguagem SQL aplicando em conjunto os comandos ALTER TABLE DELTA e SET TBLPROPERTIES. Estas informações ficam em Table Properties do catálogo da tabela delta disponível na última linha do catálogo.
# MAGIC
# MAGIC No percurso deste trabalho foi percebido a necessidade de criar uma coluna ano para cada tabela relacionada a incêndio. Na etapa final deste trabalho ficará bem evidente que a criação do campo ano é fundamental para verificar a evolução dos incêndio nas UCs entre os anos de 2018 à 2023.  Optou-se em adicionar estas colunas já dentro do formato delta através da linguagem SQL e os comentários relativos ao ano foram adicionados nos catálogos das tabelas. Os comandos utilizados foram ALTER TABLE; ADD COLUMN para criar a coluna com o tipo de dado Inteiro e, para preencher com os valores predefinido - os anos dos incêndios, UPDATE; SET. A sintaxe para as tabelas ficaram desta forma na primeira linha de código: ALTER TABLE delta./FileStore/tables/uc_2023_incendio ADD COLUMN ano INT;. Na segunda linha: UPDATE delta./FileStore/tables/uc_2023_incendio SET ano = 2023;. Este padrão se repetiu para as tabelas incêndios de outros anos.

# COMMAND ----------

caminho_tabela_delta = "/FileStore/tables/uc_atualizado_jun_2024"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_dados_uc1 USING delta LOCATION '{caminho_tabela_delta}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN TIPO_NOME TIPO_NOME STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN TIPO TIPO STRING COMMENT 'Categorias de Unidade de Conservacao abreviado ou com iniciais. Ver mais em Lei 9985/2000-SNUC'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN Nome_UC Nome_UC STRING COMMENT 'Unidade de Conservacao com descricao completa'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN NGI NGI STRING COMMENT 'Nucleo de Gestao Integrada'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN GR GR STRING COMMENT 'Gerencia Regional'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN CNUC CNUC STRING COMMENT 'Codigo CNUC (MMA)'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN Atos_legais Atos_legais STRING COMMENT 'Atos legais (Criacao ou redefinicao)'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN UF UF STRING COMMENT 'UF de Abrangencia'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN Bioma_predominante Bioma_predominante STRING COMMENT 'Bioma (IBGE 1:250mil). Para as UCs que ocorrem em mais de um bioma considera-se a que abrange 50% ou mais de seu territorio'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN Bioma_comp_RL Bioma_comp_RL STRING COMMENT 'Bioma para fins de Compensacao de Reserva Legal'")
spark.sql(f"ALTER TABLE info_dados_uc1 CHANGE COLUMN Area Area DOUBLE COMMENT 'Area em hectares'")

# COMMAND ----------

# MAGIC %md
# MAGIC ```spark.sql(f"""
# MAGIC     ALTER TABLE delta.`{caminho_tabela_delta}`
# MAGIC     SET TBLPROPERTIES (
# MAGIC         ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Atributos das Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/atributos-das-unidades-de-conservacao-federais .  Controlador: ICMBio. Mantenedor: Coordenação-Geral de Consolidação Territorial - CGTER. Frequencia de atualizacao: Quadrimestral. Ultima atualizacao: Junho/2024. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
# MAGIC     )
# MAGIC """)```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o Catálago pertinentes aos Atributos de Unidades de Conservação Federais

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_atualizado_jun_2024`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2023_incendio` ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE delta.`/FileStore/tables/uc_2023_incendio` SET ano = 2023;

# COMMAND ----------

caminho_ano_2023 = "/FileStore/tables/uc_2023_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2023 USING delta LOCATION '{caminho_ano_2023}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN NGI NGI STRING COMMENT 'Nucleo de Gestao Integrada'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Incendio Incendio DOUBLE COMMENT 'Area em hectares da UC atingida por incendio'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Queima_prescrita Queima_prescrita DOUBLE COMMENT 'Queimada prescrita em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Queima_controlada Queima_controlada DOUBLE COMMENT 'Queimada controlada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Aceiro Aceiro DOUBLE COMMENT 'Area queimada em hectares por fogo controlado para servir como barreira natural contra incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Fogo_natural Fogo_natural DOUBLE COMMENT 'Area queimada em hectares cuja as causas do fogo foram naturais'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Indigena_isolado Indigena_isolado DOUBLE COMMENT 'Area queimada em hectares por grupos indigenas isolados'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Total_prevencao Total_prevencao DOUBLE COMMENT 'Area em hectares com prevencao de incendios'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Total_combate Total_combate DOUBLE COMMENT 'Area em hectares de combate a incendios'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada total em hectares. Nao e incluido o total de area com prevencao de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2023 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2023}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")


# COMMAND ----------

# MAGIC %md
# MAGIC Será acrescentado a coluna ano para as tabelas que registram ocorrência de incêndio para o próxima etapa consulta e análise da evolução temporal dos incêndios nas UC.  

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2023  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2023_incendio`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2022_incendio`
# MAGIC ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/FileStore/tables/uc_2022_incendio`
# MAGIC SET ano = 2022;

# COMMAND ----------

caminho_ano_2022 = "/FileStore/tables/uc_2022_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2022 USING delta LOCATION '{caminho_ano_2022}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Incendio Incendio DOUBLE COMMENT 'Area em hectares da UC atingida por incendio'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Queima_prescrita Queima_prescrita DOUBLE COMMENT 'Queimada prescrita em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Queima_controlada Queima_controlada DOUBLE COMMENT 'Queimada controlada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Aceiro Aceiro DOUBLE COMMENT 'Area queimada em hectares por fogo controlado para servir como barreira natural contra incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Fogo_natural Fogo_natural DOUBLE COMMENT 'Area queimada em hectares cuja as causas do fogo foram naturais'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Queima_indigena Queima_indigena DOUBLE COMMENT 'Area queimada em hectares por grupos indigenas'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Raio Raio DOUBLE COMMENT 'Area queimada em hectares causado por raio'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada total em hectares. Nao e incluido o total de area com prevencao de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2022 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2022}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2022  

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2022_incendio`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2021_incendio` ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/FileStore/tables/uc_2021_incendio`
# MAGIC SET ano = 2021;

# COMMAND ----------

caminho_ano_2021 = "/FileStore/tables/uc_2021_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2021 USING delta LOCATION '{caminho_ano_2021}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Bioma Bioma STRING COMMENT 'Conjunto de vida vegetal e animal, constituído pelo agrupamento de tipos de vegetação que são próximos e que podem ser identificados em nível regional, com condições de geologia e clima semelhantes e que, historicamente, sofreram os mesmos processos de formação da paisagem - https://educa.ibge.gov.br/criancas/brasil/nosso-territorio/19635-ecossistemas.html'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Fogo_natural Fogo_natural DOUBLE COMMENT 'Area queimada em hectares cuja as causas do fogo foram naturais'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Queima_indig_isol Queima_indig_isol DOUBLE COMMENT 'Area queimada em hectares por grupos indigenas isolados'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Incendio Incendio DOUBLE COMMENT 'Area em hectares da UC atingida por incendio'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Queima_controlada Queima_controlada DOUBLE COMMENT 'Queimada controlada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Queima_prescrita Queima_prescrita DOUBLE COMMENT 'Queimada prescrita em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Gestao_raio Gestao_raio DOUBLE COMMENT 'Area queimada em hectares causado por raio'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Aceiro_negro Aceiro_negro DOUBLE COMMENT 'Area queimada em hectares por fogo controlado para servir como barreira natural contra incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Data_ult_atual Data_ult_atual DATE COMMENT 'Data da ultima atualizacao do conjunto de dados original'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN Satelite Satelite STRING COMMENT 'Imagem de satelite utilizada para identificar focos de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2021 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2021}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2021

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2021_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2020_incendio` ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/FileStore/tables/uc_2020_incendio`
# MAGIC SET ano = 2020;

# COMMAND ----------

caminho_ano_2020 = "/FileStore/tables/uc_2020_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2020 USING delta LOCATION '{caminho_ano_2020}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Bioma Bioma STRING COMMENT 'Conjunto de vida vegetal e animal, constituído pelo agrupamento de tipos de vegetação que são próximos e que podem ser identificados em nível regional, com condições de geologia e clima semelhantes e que, historicamente, sofreram os mesmos processos de formação da paisagem - https://educa.ibge.gov.br/criancas/brasil/nosso-territorio/19635-ecossistemas.html'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Incendio Incendio DOUBLE COMMENT 'Area em hectares da UC atingida por incendio'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN MIF MIF DOUBLE COMMENT 'Area em hectares do Manejo Imtegrado do Fogo'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Data_ult_atual Data_ult_atual DATE COMMENT 'Data da ultima atualizacao do conjunto de dados original'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN Satelite Satelite STRING COMMENT 'Imagem de satelite utilizada para identificar focos de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2020 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2020}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2020

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2020_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2019_incendio` ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/FileStore/tables/uc_2019_incendio`
# MAGIC SET ano = 2019;

# COMMAND ----------

caminho_ano_2019 = "/FileStore/tables/uc_2019_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2019 USING delta LOCATION '{caminho_ano_2019}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Bioma Bioma STRING COMMENT 'Conjunto de vida vegetal e animal, constituído pelo agrupamento de tipos de vegetação que são próximos e que podem ser identificados em nível regional, com condições de geologia e clima semelhantes e que, historicamente, sofreram os mesmos processos de formação da paisagem - https://educa.ibge.gov.br/criancas/brasil/nosso-territorio/19635-ecossistemas.html'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Data_ult_atual Data_ult_atual DATE COMMENT 'Data da ultima atualizacao do conjunto de dados original'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN Satelite Satelite STRING COMMENT 'Imagem de satelite utilizada para identificar focos de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2019 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2019}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2019

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2019_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE delta.`/FileStore/tables/uc_2018_incendio` ADD COLUMN ano INT;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE delta.`/FileStore/tables/uc_2018_incendio`
# MAGIC SET ano = 2018;

# COMMAND ----------

caminho_ano_2018 = "/FileStore/tables/uc_2018_incendio"

# Registrar a tabela Delta no catálogo do Spark
spark.sql(f"CREATE TABLE info_incendio_2018 USING delta LOCATION '{caminho_ano_2018}'")

# COMMAND ----------

spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN ind ind INT COMMENT 'index gerado'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN UC UC STRING COMMENT 'Unidade de Conservacao com a Categoria abreviada ou com iniciais'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Bioma Bioma STRING COMMENT 'Conjunto de vida vegetal e animal, constituído pelo agrupamento de tipos de vegetação que são próximos e que podem ser identificados em nível regional, com condições de geologia e clima semelhantes e que, historicamente, sofreram os mesmos processos de formação da paisagem - https://educa.ibge.gov.br/criancas/brasil/nosso-territorio/19635-ecossistemas.html'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Area_queimada Area_queimada DOUBLE COMMENT 'Area Queimada em hectares'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Area_UC Area_UC DOUBLE COMMENT 'Area em hectares da UC'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Perc_UC Perc_UC DOUBLE COMMENT 'Percentual de area afetada pelo incendio em relacao a area da UC'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Data_ult_atual Data_ult_atual DATE COMMENT 'Data da ultima atualizacao'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN Satelite Satelite STRING COMMENT 'Imagem de satelite utilizada para identificar focos de incendios florestais'")
spark.sql(f"ALTER TABLE info_incendio_2018 CHANGE COLUMN ano ano INT COMMENT 'Ano da ocorrencia do incendio'")

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE delta.`{caminho_ano_2018}`
    SET TBLPROPERTIES (
        ' Info' = 'Tabela modificada cujo dados sao provenientes do dataset Áreas Queimadas em Unidades de Conservação Federais disponivel em https://dados.gov.br/dados/conjuntos-dados/incendios-em-ucs .  Controlador ICMBio. Mantenedor: Divisão de Informações Geoespaciais e Monitoramento - DGEO. Frequencia de atualizacao: Anual. Ultima atualizacao: Outubro/2023. Modificado por Ana Paula Pessoa da Silva para entrega de trabalho de Eng. De Dados da Pos-Graduacao em Ciencia de Dados e Analiticas do PUC-Rio de Janeiro entre periodo de junho a julho deste ano (2024)  '
    )
""")

# COMMAND ----------

# MAGIC %md
# MAGIC Visualizando o catálago pertinentes aos Dados de Registro de Incendios nas UCs de 2018

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED delta.`/FileStore/tables/uc_2018_incendio`;

# COMMAND ----------

# MAGIC %md
# MAGIC 3 - Extração de Dados - Consultas

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Agora é o momento de realizar as consultas para extrair as informações a respeito das ocorrências de incêndios. Para isto, as tabelas deltas serão lidas como janelas temporárias.  Além disso, serão criadas tabelas temporárias de ocorrencia de incêndio com mesmo esquema para viabilizar a reunião destas em uma, só através do comando UNION ALL do SQL      

# COMMAND ----------


spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2018_incendio").createOrReplaceTempView("incendio_2018")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2019_incendio").createOrReplaceTempView("incendio_2019")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2020_incendio").createOrReplaceTempView("incendio_2020")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2021_incendio").createOrReplaceTempView("incendio_2021")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2022_incendio").createOrReplaceTempView("incendio_2022")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_2023_incendio").createOrReplaceTempView("incendio_2023")
spark.read.format("delta").load("dbfs:/FileStore/tables/uc_atualizado_jun_2024").createOrReplaceTempView("dados_uc")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW DADOS_UC AS
# MAGIC SELECT *
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_atualizado_jun_2024`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2023 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2023_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2022 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2022_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2021 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2021_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2020 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2020_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2019 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2019_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCEND_2018 AS
# MAGIC SELECT UC, Area_queimada, Perc_UC, ano
# MAGIC FROM delta.`dbfs:/FileStore/tables/uc_2018_incendio`;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HISTORICO AS
# MAGIC SELECT *
# MAGIC FROM INCEND_2023
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM INCEND_2022
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM INCEND_2021
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM INCEND_2020
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM INCEND_2019
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM INCEND_2018
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Aproveitando que foi criado uma tabela temporaria única para as ocorrências de incêndio será feito uma outra acrescentando os atributos da Tabela com Informações Gerais das Unidades de Conservação.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW HISTORICO_DADOS_UC AS
# MAGIC SELECT CNUC, TIPO_NOME, NOME_UC, TIPO, Bioma_predominante, UF, Area_queimada, Perc_UC, ano
# MAGIC FROM DADOS_UC
# MAGIC INNER JOIN HISTORICO
# MAGIC ON TIPO_NOME = UC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --este esquema seria o ideal para a tabela de incêndio.  Uma tabela única com uma coluna ano que identifique a ocorrência do incêndio e com id das UC, a CNUC
# MAGIC SELECT *
# MAGIC FROM HISTORICO_DADOS_UC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Vamos iniciar as consultas.  Primeira: pergunta quais foram as cinco UCs mais afetadas por incêndios no ano de 2018? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Somar valores de vendas por categoria
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2018, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2018
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2018
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2018 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC E quais foram as cinco UCs mais afetadas por incêndios no ano de 2019? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Somar valores de vendas por categoria
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2019, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2019
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2019
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2019 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC E quais foram as cinco UCs mais afetadas por incêndios no ano de 2020? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Somar valores de vendas por categoria
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2020, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2020
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2020
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2020 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC E quais foram as cinco UCs mais afetadas por incêndios no ano de 2021? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Somar valores de vendas por categoria
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2021, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2021
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2021
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2021 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC E quais foram as cinco UCs mais afetadas por incêndios no ano de 2022? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Somar valores de vendas por categoria
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2022, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2022
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2022
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2022 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC E quais foram as cinco UCs mais afetadas por incêndios no ano de 2023? Apresentar a resposta com o bioma predominante.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT NOME_UC, Bioma_predominante, SUM(Area_queimada) AS Maiores_Areas_Queimadas_2023, AVG(PERC_UC) AS Media_Percentual_AreaUC_Atingida_2023
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC WHERE ano = 2023
# MAGIC GROUP BY NOME_UC, Bioma_predominante
# MAGIC ORDER BY Maiores_Areas_Queimadas_2023 DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC Pelas respostas é possível afirmar que o Biomas da UCs mais atingido por incêndios é o Cerrado. O Pantanal chama atenção pelo elevado percentual de área atingida em relação à área total da UC, o Parque Nacional do Pantanal Mato-Grossense. Em 2020 foi próximo de 100%, algo que impressiona. Em 2023, o percentual foi menor, próximo de 40%, mas ainda assim, signifativo. Com relação a persistência do problema entre o período de 2018-2023, a Estação Ecológica Serra Geral do Tocantins manteve entre na liderança juntamente com O Parque Nacional do Araguaia.  São duas UCs que exigem maior atenção para a questão analisada aqui.  

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC São muita perguntas que podem ser feitas, mas vamos encerrar duas perguntas finais. A primeira é para identificar quais as Unidades Conservação entre o período de 2018-2023 não registraram ocorrências de incêndios.  É importante destacar que por não existir uma coluna específica de ano de criação legal das Unidades de Conservação, o resultado será aproximado, pois não é possível registros de ocorrências em período em que uma UCs não existia. Logo UCs criadas em 2024, retornará como resposta, o que não significa que não teve o problema.  Ainda assim, é melhor este tipo consulta do que verificar qual foi a menor área atingida por queimada em Unidade de Conservação. O motivo disto é muito simples: uma Unidade de Conservação server para proteção ambiental integral ou para manejo sustentável, ou seja, este lugares não é para sofrer com incêndios florestais, não importa a dimensão da área.
# MAGIC
# MAGIC A outra consulta é soma total das áreas queimadas por ano para visualização de como evolui as ocorrências de incêndios ao longo do referido período.  

# COMMAND ----------

# MAGIC %sql
# MAGIC --UCs não atingidas por incêndios. Observação: verificar a data de criação da Unidade de Conservação para validação da resposta
# MAGIC SELECT TIPO_NOME, Atos_legais
# MAGIC FROM DADOS_UC
# MAGIC WHERE TIPO_NOME NOT IN 
# MAGIC                 (SELECT TIPO_NOME
# MAGIC                 FROM HISTORICO_DADOS_UC)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Gráfico que registra a evolução das áreas Queimadas entre o período de 2018 à 2023
# MAGIC SELECT SUM(Area_queimada) AS AREA_QUEIM_ANO, ano
# MAGIC FROM HISTORICO_DADOS_UC
# MAGIC GROUP BY ano

# COMMAND ----------

# MAGIC %md
# MAGIC Pelos gráficos acima é possível perceber que apesar do aumento de registros de ocorrência de incêndios em UCs, houve uma diminuição das áreas atingidas, exceto na comparação entre o ano de 2018 e 2023. Dentro deste período, o ano de 2020, foi pior ano com maior área atingida por incêndios em Unidades de Conservação. 

# COMMAND ----------

# MAGIC %md
# MAGIC <div style="text-align: justify;">
# MAGIC 4 - Encerramento do Trabalho e Conclusão 
# MAGIC
# MAGIC O trabalho se iniciou pelo tratamento dos arquivos em formato xlsx no Colab do Google Driver. Após isto, os arquivos foram convertidos em CSV, armazenados no GitHub e foram carregados para dentro do Plataforma Databricks. Após o upload, foram verificado quais ajustes e correções necessárias que foram realizadas já com as tabelas em formato delta. O ajuste final foi acréscimo da coluna ano para cada tabela de ocorrências, o que foi fundamental na etapa final de união de várias tabelas em uma apenas.  Durante a consulta constatou-se que nas Unidades de Conservação, o Bioma mais afetado é o Cerrado.  O Pantanal não esteve presente em todos os anos do período entre 2018 à 2023, mas quando aparece, especialmente em 2020, surge com percentual alarmante de área afetada em relação a área de abragência da UC. No gráfico de evolução, é possível perceber foi o pior ano com as maiores áreas atingidas pelo fogo.  
# MAGIC
# MAGIC Foi interessante desenvolver este projeto e trabalhar com dados que me são muito caros. Acredito que os objetivos do trabalho foram alcançados e que foi cumprido todo o ciclo de vida de produção do dado, apesar do curtíssimo espaço de tempo para aprender a utilizar o Databricks, uma plataforma que até então desconhecia. Reconheço que oferece bastantes recursos mesmo na versão gratuita, dois que cito é a possibilidade de usar diferentes linguagens em um só notebook e a outra é a facilidade de gerar a visualização dos dados pelos gráficos. Pontos que entendo que precisa de melhorr está relacionado a duas colunas Ato_legal e UF da tabela Atributos das Unidades de Conservação.  É necessário ter uma coluna ano_ato_legal para ser possível realizar filtros necessários como a que ocorreu com consulta de Unidades não afetadas por incêndios. E, no caso da UF, as linhas armazenam multiplos valores, sendo que o ideal é uma tabela onde há a parte onde cada UF diferente dentro de uma UC seja uma linha. Oportunamente serão adotadas estas melhorias, pois a ideia é continuar alimentado este banco de dados relativos aos incêndios a medida que a ICMBio for atualizando os dados no site GOV.BR.  
# MAGIC
# MAGIC Encerro na torcida que os políticos se sensibilizem para a questão ambiental, que é urgente, e parem de cortar orçamento para a Pasta Ambiental. O futuro do nosso país agradece. 
