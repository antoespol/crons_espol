import pandas as pd
import os

from google.cloud import bigquery

PATH_WH = os.getenv('PATH_WH', '')
PATH_POS_ATT = os.getenv('PATH_POS_ATT', '')


def load_warehouse_data(path):
    df = pd.read_csv(
        path,
        encoding='utf-8',
        dtype={
            'lote': 'str',
        }
    )
    df = df.drop_duplicates()
    df['fecha_vencimiento'] = pd.to_datetime(df['fecha_vencimiento'], format='%Y-%m-%d')
    return df


def load_pos_att(path):
    df = pd.read_csv(
        path,
        encoding='utf-8'
    )
    df = df.drop_duplicates()
    prefix = 'pos_'
    df.columns = [prefix + col if col != 'posicion' else col for col in df.columns]
    return df
    

def add_extra_columns(df):
    df['created_at'] = pd.to_datetime('today').date()
    df['turno'] = 'matutino' if pd.to_datetime('now', utc=True).tz_convert('America/Santiago').hour < 12 else 'tarde'
    bool_cols = [
        'sensible', 'inflamable', 'perfumante', 'sensible_temperatura',
        'liquido', 'fragil', 'pos_sensible', 'pos_inflamable',
        'pos_perfumantes', 'pos_sensible_temperatura', 'pos_pulmon'
    ]
    for col in bool_cols:
        df[col] = df[col].fillna(0).astype(int)
    return df


def upload_to_bigquery(df, table_id, project_id=None, write_disposition="WRITE_APPEND"):
    client = bigquery.Client(project=project_id)
    table_ref = client.get_table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Datos subidos exitosamente a {table_id}")


def main():
    df_wh = load_warehouse_data(PATH_WH)
    df_pos_att = load_pos_att(PATH_POS_ATT)
    df_wh = df_wh.merge(
        df_pos_att,
        how='left',
        on='posicion'
    )
    df_wh = add_extra_columns(df_wh)
    upload_to_bigquery(df_wh, 'data_espol.stock_wms_historico', 'analitica-avanzada-338715')


if __name__ == "__main__":
    main()


