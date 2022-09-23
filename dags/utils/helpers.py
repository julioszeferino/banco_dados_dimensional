import boto3
import redshift_connector

_model: list = [
    "dim_calendario",
    "dim_carros_marcas",
    "dim_carros_modelos",
    "dim_carros",
    "dim_cidades",
    "dim_clientes",
    "fato_apolices",
    "fato_sinistros",
]


def _load_s3(key: str, secret: str) -> None:

    _s3_client = boto3.client(
        's3',
        aws_access_key_id=key,
        aws_secret_access_key=secret
    )

    for arquivo in _model:

        _s3_client.upload_file(
            f'/opt/airflow/data/processed/{arquivo}.csv',
            "etl-seguros",
            f'{arquivo}.csv'
        )

    _s3_client.close()


def _create_tables_redshift(user_db: str, passwd_db: str) -> None:
    
    _conn = redshift_connector.connect(
        host='redshift-etl-seguros.c0hh2rcmul4u.us-east-1.redshift.amazonaws.com',
        port=5439,
        database='seguros',
        user=user_db,
        password=passwd_db
    )

    with _conn.cursor() as cursor:

        cursor.execute("DROP TABLE IF EXISTS dim_calendario CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_carros_marcas CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_carros_modelos CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_carros CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_cidades CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS dim_clientes CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS fato_apolices CASCADE;")
        cursor.execute("DROP TABLE IF EXISTS fato_sinistros CASCADE;")
    
        print("Criando a tabela dim_calendario..")
        cursor.execute("""
        CREATE TABLE dim_calendario (
            data DATE NOT NULL,
            ano INT NOT NULL,
            mes INT NOT NULL,
            dia INT NOT NULL,
            nome_dia_semana VARCHAR(3) NOT NULL,
            nome_mes VARCHAR(15) NOT NULL,
            id_calendario BIGINT NOT NULL,
            CONSTRAINT dim_calendario_pk PRIMARY KEY (id_calendario)
        );
        """)

        print("Criando a tabela dim_carros_marcas..")
        cursor.execute("""
        CREATE TABLE dim_carros_marcas (
            marca VARCHAR(50) NOT NULL,
            data_criacao TIMESTAMP NOT NULL,
            id_carro_marca BIGINT NOT NULL DISTKEY SORTKEY,
            CONSTRAINT dim_carros_marcas_pk PRIMARY KEY (id_carro_marca)
        );
        """)

        print("Criando a tabela dim_carros_modelos..")
        cursor.execute("""
        CREATE TABLE dim_carros_modelos (
            modelo VARCHAR(50) NOT NULL,
            data_criacao TIMESTAMP NOT NULL,
            id_carro_modelo BIGINT NOT NULL DISTKEY SORTKEY,
            CONSTRAINT dim_carros_modelos_pk PRIMARY KEY (id_carro_modelo)
        );  
        """)

        print("Criando a tabela dim_carros..")
        cursor.execute("""
        CREATE TABLE dim_carros (
            id_carro BIGINT NOT NULL DISTKEY SORTKEY,
            placa VARCHAR(15) NOT NULL,
            data_criacao TIMESTAMP NOT NULL,
            CONSTRAINT dim_carros_pk PRIMARY KEY (id_carro)
        );
        """)

        print("Criando a tabela dim_cidades..")
        cursor.execute("""
        CREATE TABLE dim_cidades (
            cidade VARCHAR(150) NOT NULL,
            data_criacao TIMESTAMP NOT NULL,
            id_cidade BIGINT NOT NULL DISTKEY SORTKEY,
            CONSTRAINT dim_cidades_pk PRIMARY KEY (id_cidade)
        );
        """)

        print("Criando a tabela dim_clientes..")
        cursor.execute("""
        CREATE TABLE dim_clientes (
            id_cliente BIGINT NOT NULL DISTKEY SORTKEY,
            nome TEXT NOT NULL,
            cpf VARCHAR(15) NOT NULL,
            data_criacao TIMESTAMP NOT NULL,
            CONSTRAINT dim_clientes_pk PRIMARY KEY (id_cliente)
        );
        """)

        print("Criando a tabela fato_apolices..")
        cursor.execute("""
        CREATE TABLE fato_apolices (
            sk_cliente BIGINT NOT NULL,
            sk_carro BIGINT NOT NULL,
            qtde_apolices INT NOT NULL,
            qtde_apolices_vigentes INT NOT NULL,
            id_apolice BIGINT NOT NULL,
            CONSTRAINT fato_apolices_pk PRIMARY KEY (id_apolice)
        );
        """)

        print("Criando a tabela fato_sinistros..")
        cursor.execute("""
        CREATE TABLE fato_sinistros (
            sk_data BIGINT NOT NULL DISTKEY SORTKEY,
            sk_cidade BIGINT NOT NULL,
            sk_carro_marca BIGINT NOT NULL,
            sk_carro_modelo BIGINT NOT NULL,
            sk_cliente BIGINT NOT NULL, 
            sk_carro BIGINT NOT NULL,
            qtde_sinistros INT NOT NULL,
            id_sinistro BIGINT NOT NULL,
            CONSTRAINT fato_sinistros_pk PRIMARY KEY (id_sinistro)
        );
        """)

        print("Criando as FKs..")
        cursor.execute("ALTER TABLE fato_apolices ADD CONSTRAINT fato_apolices_dim_clientes_fk FOREIGN KEY( sk_cliente ) REFERENCES dim_clientes ( id_cliente );")
        cursor.execute("ALTER TABLE fato_apolices ADD CONSTRAINT fato_apolices_dim_carros_fk FOREIGN KEY( sk_carro ) REFERENCES dim_carros ( id_carro );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_clientes_fk FOREIGN KEY( sk_cliente ) REFERENCES dim_clientes ( id_cliente );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_carros_fk FOREIGN KEY( sk_carro ) REFERENCES dim_carros ( id_carro );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_carros_marcas_fk FOREIGN KEY( sk_carro_marca ) REFERENCES dim_carros_marcas ( id_carro_marca );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_carros_modelos_fk FOREIGN KEY( sk_carro_modelo ) REFERENCES dim_carros_modelos ( id_carro_modelo );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_cidades_fk FOREIGN KEY( sk_cidade ) REFERENCES dim_cidades ( id_cidade );")
        cursor.execute("ALTER TABLE fato_sinistros ADD CONSTRAINT fato_sinistros_dim_calendario_fk FOREIGN KEY( sk_data ) REFERENCES dim_calendario ( id_calendario );")

        _conn.commit()
        _conn.close()


def _load_redshift(user_db: str, passwd_db: str) -> None:

    _conn = redshift_connector.connect(
        host='redshift-etl-seguros.c0hh2rcmul4u.us-east-1.redshift.amazonaws.com',
        port=5439,
        database='seguros',
        user=user_db,
        password=passwd_db
    )

    _cursor = _conn.cursor()

    for tabela in _model:

        print(f"Carregando a tabela {tabela}..")

        _cursor.execute(f"TRUNCATE TABLE {tabela};")

        _cursor.execute(f"""
        COPY {tabela} 
        FROM 's3://etl-seguros/{tabela}.csv' 
        IAM_ROLE 'arn:aws:iam::878845310646:role/myRedshiftRole' 
        FORMAT AS CSV DELIMITER ';'
        IGNOREHEADER 1
        DATEFORMAT AS 'YYYY-MM-DD';
        """)

    _conn.commit()
    _conn.close()