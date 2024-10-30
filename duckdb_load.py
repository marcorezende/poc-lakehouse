import duckdb


def get_time_series_data():
    df = duckdb.sql("""SELECT * FROM time_series.csv""")
    return df.to_df()


def get_data():
    duckdb.sql("""
    CREATE OR REPLACE SECRET my_secret (
        TYPE S3,
        ENDPOINT 'localhost:9000',
        REGION 'us-east-1',
        KEY_ID 'TESTE123',
        SECRET 'TESTE123',
        USE_SSL 'false',
        URL_STYLE 'path')
    """)

    fato_incidente_path = 's3://bucket-teste/gold/incident/fato_incidente/'
    df_equipamento_path = 's3://bucket-teste/gold/incident/dim_equipamento/'
    df_turno_path = 's3://bucket-teste/gold/incident/dim_turno/'
    df_tempo_path = 's3://bucket-teste/gold/incident/dim_tempo/'
    df_local_path = 's3://bucket-teste/gold/incident/dim_local/'
    df_alarme_path = 's3://bucket-teste/gold/incident/dim_alarme/'

    duckdb.sql(f"CREATE OR REPLACE VIEW fato_incidente AS SELECT * FROM delta_scan('{fato_incidente_path}')")
    duckdb.sql(f"CREATE OR REPLACE VIEW dim_equipamento AS SELECT * FROM delta_scan('{df_equipamento_path}')")
    duckdb.sql(f"CREATE OR REPLACE VIEW dim_turno AS SELECT * FROM delta_scan('{df_turno_path}')")
    duckdb.sql(f"CREATE OR REPLACE VIEW dim_tempo AS SELECT * FROM delta_scan('{df_tempo_path}')")
    duckdb.sql(f"CREATE OR REPLACE VIEW dim_local AS SELECT * FROM delta_scan('{df_local_path}')")
    duckdb.sql(f"CREATE OR REPLACE VIEW dim_alarme AS SELECT * FROM delta_scan('{df_alarme_path}')")

    query = '''
        SELECT
            f.duracao_incidente,
            e.nome AS equipamento_nome,
            e.modelo AS equipamento_modelo,
            e.nome_grupo AS equipamento_grupo,

            t.nome AS turno,

            tp.data_inicio,
            tp.data_final,
            tp.data,

            l.nome AS local_nome,
            l.secao,

            a.nome AS alarme_nome,
            a.codigo AS alarme_codigo,
            a.level AS alarme_level

        FROM fato_incidente f

        LEFT JOIN dim_equipamento e ON f.sk_equipamento = e.sk_equipamento
        LEFT JOIN dim_turno t ON f.sk_turno = t.sk_turno
        LEFT JOIN dim_tempo tp ON f.sk_tempo = tp.sk_tempo
        LEFT JOIN dim_local l ON f.sk_local = l.sk_local
        LEFT JOIN dim_alarme a ON f.sk_alarme = a.sk_alarme
    '''

    df = duckdb.sql(query)
    return df.to_df()
