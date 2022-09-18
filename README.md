# Criacao de um Banco de Dados Dimensional

> Projeto pratico em modo de desafio com a criacao de um banco de dados dimensional (Cubo OLAP) desenvolvido como atividade avaliativa do Bootcamp Engenheiro de Dados da XPEducacao.

O modelo dimensional foi criado a partir das metricas propostas pelo desafio.

Visando tornar o projeto mais robusto, criou-se um pipeline de dados orquestrado pelo **Apache Airflow** que recupera os dados em formato `.csv`, realiza os procedimentos de transformacao e compatibilizacao dos dados com o modelo dimensional desenhado.

Os dados modelados sao colocados em um bucket da **AWS S3**, utilizado como *staging area* e, por fim, os dados sao armazenados em um cluster na **AWS Redshift**.

Os dados do Redshift sao consumidos pelo **Power BI**, onde sera criado um dashboard de acordo com as metricas e necessidades de negocio levantadas.

## **Ambiente de Desenvolvimento**

[Docker 20.10.17](https://www.docker.com/)  

## **Como Executar este Projeto**

1. Crie uma instancia no `AWS EC2` ou outro provedor da sua escolha, realize o download deste repositorio e execute o docker-compose para realizar o build dos containers:

```bash
docker-compose up -d
```

2. No painel web do `Apache Airflow` cadastrar as conexoes do `AWS S3` com o nome *aws_s3* e do `AWS Redshift` com o nome *aws_redshift*.  
3. Criar as tabelas necessarias no `AWS Redshift` de acordo com script create_database.sql
4. Criar um bucket no `AWS S3` com dois niveis: raw e processed
5. Executar o pipeline de dados no `Apache Airflow`
6. Configurar e criar as visualizacoes no `Apache Superset`


## **Referencias**

A documentacao completa do projeto esta disponivel neste link: https://julioszeferino.github.io/banco_de_dados_dimensional/ 

## Histórico de Atualizações

* 0.0.1
    * Projeto Inicial

## Direitos de Uso
A ideia deste repositório é treinar os conceitos de Pipelines de Dados e compartilhar conhecimento. Dessa forma, você pode replicar e utilizar o conteúdo deste repositório sem nenhuma restrição desde que forneça uma atribuição de volta e não me responsabilize por quaisquer reclamações, danos ou responsabilidades.  

Exigido | Permitido |Proibido
:---: | :---: | :---:
Aviso de licença e direitos autorais | Uso comercial | Responsabilidade Assegurada
 || Modificação ||
 || Distribuição || 
 || Sublicenciamento ||

## Meta

Seu nome - [@SeuTwitter](https://twitter.com/julioszeferino) - julioszeferino@gmail.com
[https://github.com/julioszeferino]