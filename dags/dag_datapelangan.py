import logging
import csv
import pendulum
import pandas as pd
from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Define the Airflow Connection ID you will create in the UI
# This connection will point to your 'dwh_prod' database
DWH_CONN_ID = "postgres_dwh"
INPUT_DIR = "/opt/airflow/data/input"
OUTPUT_DIR = "/opt/airflow/data/output"
local_tz = pendulum.timezone("Asia/Jakarta")

@task
def init_schemas():
    """
    Connects to the dwh_prod database and creates
    staging and cube schemas and tables in them if they don't exist.
    """
    pg_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        logging.info("Creating schema 'staging' if not exists...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS staging;")

        logging.info("Creating schema 'cube' if not exists...")
        cursor.execute("CREATE SCHEMA IF NOT EXISTS cube;")

        logging.info("Creating table 'staging.dummy_union_transaksi' if not exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.dummy_union_transaksi (
                "uuid" text NULL,
                waktu_transaksi date NULL,
                armada_id_var text NULL,
                no_body_var text NULL,
                shelter_name_var text NULL,
                terminal_name_var text NULL,
                card_number_var int8 NULL,
                card_type_var text NULL,
                balance_before_int int8 NULL,
                fare_int int8 NULL,
                balance_after_int int8 NULL,
                transcode_txt text NULL,
                gate_in_boo bool NULL,
                p_latitude_flo numeric NULL,
                p_longitude_flo numeric NULL,
                status_var text NULL,
                free_service_boo bool NULL,
                insert_on_dtm text NULL,
                route_code text NULL,
                route_name text NULL,
                transaction_source text NULL
            );"""
        )

        logging.info("Creating table 'cube.dummy_agg_by_card_type' if not exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cube.dummy_agg_by_card_type (
                waktu_transaksi date NULL,
                card_type_var text NULL,
                gate_in_boo bool NULL,
                jumlah_pelanggan int8 NULL,
                jumlah_amount int8 NULL
            );"""
        )

        logging.info("Creating table 'cube.dummy_agg_by_route' if not exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cube.dummy_agg_by_route (
                waktu_transaksi date NULL,
                route_code text NULL,
                route_name text NULL,
                gate_in_boo bool NULL,
                jumlah_pelanggan int8 NULL,
                jumlah_amount int8 NULL
            );"""
        )

        logging.info("Creating table 'cube.dummy_agg_by_tarif' if not exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cube.dummy_agg_by_tarif (
                waktu_transaksi date NULL,
                fare_int int8 NULL,
                gate_in_boo bool NULL,
                jumlah_pelanggan int8 NULL,
                jumlah_amount int8 NULL
            );"""
        )

        conn.commit()
        logging.info("Schemas 'staging' and 'cube' and tables in them are ready.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error creating schemas/tables: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

@task
def load_to_staging():
    """
    Task 1: Load CSVs to staging schema.
    """
    logging.info("Starting load to staging...")
    pg_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    
    # dummy_routes.csv
    routes_df = pd.read_csv(f"{INPUT_DIR}/dummy_routes.csv")
    routes_df.to_sql(
        name="dummy_routes",
        con=pg_hook.get_sqlalchemy_engine(),
        schema="staging",  
        if_exists="replace",
        index=False
    )
    logging.info("Loaded dummy_routes to staging.dummy_routes")

    # dummy_realisasi_bus.csv
    realisasi_bus_df = pd.read_csv(f"{INPUT_DIR}/dummy_realisasi_bus.csv")
    realisasi_bus_df['tanggal_realisasi'] = pd.to_datetime(realisasi_bus_df['tanggal_realisasi']).dt.date
    realisasi_bus_df.to_sql(
        name="dummy_realisasi_bus",
        con=pg_hook.get_sqlalchemy_engine(),
        schema="staging",  
        if_exists="replace",
        index=False
    )
    logging.info("Loaded dummy_realisasi_bus to staging.dummy_realisasi_bus")

    # dummy_shelter_corridor.csv
    shelter_corridor_df = pd.read_csv(f"{INPUT_DIR}/dummy_shelter_corridor.csv")
    shelter_corridor_df['corridor_code'] = shelter_corridor_df['corridor_code'].astype(str)
    shelter_corridor_df.to_sql(
        name="dummy_shelter_corridor",
        con=pg_hook.get_sqlalchemy_engine(),
        schema="staging",  
        if_exists="replace",
        index=False
    )
    logging.info("Loaded dummy_shelter_corridor to staging.dummy_shelter_corridor")

    # dummy_transaksi_bus.csv
    transaksi_bus_df = pd.read_csv(f"{INPUT_DIR}/dummy_transaksi_bus.csv")
    transaksi_bus_df.to_sql(
        name="dummy_transaksi_bus",
        con=pg_hook.get_sqlalchemy_engine(),
        schema="staging",  
        if_exists="replace",
        index=False
    )
    logging.info("Loaded dummy_transaksi_bus to staging.dummy_transaksi_bus")

    # dummy_transaksi_halte.csv
    transaksi_halte_df = pd.read_csv(f"{INPUT_DIR}/dummy_transaksi_halte.csv")
    transaksi_halte_df.to_sql(
        name="dummy_transaksi_halte",
        con=pg_hook.get_sqlalchemy_engine(),
        schema="staging",  
        if_exists="replace",
        index=False
    )
    logging.info("Loaded dummy_transaksi_halte to staging.dummy_transaksi_halte")

    logging.info("All CSVs loaded to staging.")

@task
def transform_in_postgres(ds, **kwargs):
    """
    Task 2: Run transformations using SQL.
    This runs SQL directly in PostgreSQL as requested.
    """
    logging.info("Starting transformation in PostgreSQL...")
    pg_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    
    # This SQL should perform the following logic:
    # 1. Join staging tables
    # 2. Standardize no_body_var 
    # 3. Handle duplicates 
    # 4. Aggregate by card_type, route, tarif 
    # 5. Load into cube tables
    
    sql_dummy_union_transaksi = """
    delete from staging.dummy_union_transaksi WHERE waktu_transaksi::date = '{DATE_FILTER}';
    insert into staging.dummy_union_transaksi
    with union_trx as (
        select distinct
            dtb.uuid,
            dtb.waktu_transaksi::date AS waktu_transaksi,
            dtb.armada_id_var,
            SUBSTRING(dtb.no_body_var, 1, 3) || '-' || LPAD(regexp_replace(substring(dtb.no_body_var, 4),'[^0-9]','', 'g'), 3, '0') as no_body_var ,
            null as shelter_name_var,
            null as terminal_name_var,
            dtb.card_number_var,
            dtb.card_type_var,
            dtb.balance_before_int,
            dtb.fare_int,
            dtb.balance_after_int,
            dtb.transcode_txt,
            dtb.gate_in_boo,
            dtb.p_latitude_flo ,
            dtb.p_longitude_flo ,
            dtb.status_var,
            dtb.free_service_boo ,
            dtb.insert_on_dtm ,
            drb.rute_realisasi as route_code,
            dr.route_name,
            'BUS' AS transaction_source
        from staging.dummy_transaksi_bus dtb
        left join staging.dummy_realisasi_bus drb on dtb.waktu_transaksi::date = drb.tanggal_realisasi and drb.bus_body_no = dtb.no_body_var 
        left join staging.dummy_routes dr on dr.route_code = drb.rute_realisasi 
        where status_var = 'S' and dtb.waktu_transaksi::date = '{DATE_FILTER}'
        union all
        select distinct
            dth.uuid,
            dth.waktu_transaksi::date AS waktu_transaksi,
            null as armada_id_var,
            null as no_body_var,
            dth.shelter_name_var,
            dth.terminal_name_var,
            dth.card_number_var,
            dth.card_type_var,
            dth.balance_before_int,
            dth.fare_int,
            dth.balance_after_int,
            dth.transcode_txt,
            dth.gate_in_boo,
            dth.p_latitude_flo ,
            dth.p_longitude_flo ,
            dth.status_var,
            dth.free_service_boo ,
            dth.insert_on_dtm ,
            dsc.corridor_code as route_code,
            dr.route_name,
            'HALTE' AS transaction_source
        from staging.dummy_transaksi_halte dth
        left join staging.dummy_shelter_corridor dsc on dsc.shelter_name_var = dth.shelter_name_var 
        left join staging.dummy_routes dr on dr.route_code = dsc.corridor_code
        where status_var = 'S' and dth.waktu_transaksi::date = '{DATE_FILTER}'
    )
    select * from union_trx;
    """
    pg_hook.run(sql_dummy_union_transaksi.format(DATE_FILTER=ds))
    logging.info("Transformation complete. Created staging.dummy_union_transaksi.")
    
    sql_dummy_agg_by_card_type = """
    delete from cube.dummy_agg_by_card_type WHERE waktu_transaksi::date = '{DATE_FILTER}';
    insert into cube.dummy_agg_by_card_type
    select
		waktu_transaksi ,
		card_type_var ,
		gate_in_boo ,
		count(distinct card_number_var) as jumlah_pelanggan ,
		sum(fare_int) as jumlah_amount
	from staging.dummy_union_transaksi
    where waktu_transaksi::date = '{DATE_FILTER}'
    group by 1, 2, 3;
    """
    pg_hook.run(sql_dummy_agg_by_card_type.format(DATE_FILTER=ds))
    logging.info("Transformation complete. Created cube.dummy_agg_by_card_type.")
    
    sql_dummy_agg_by_route = """
    delete from cube.dummy_agg_by_route WHERE waktu_transaksi::date = '{DATE_FILTER}';
    insert into cube.dummy_agg_by_route
    select 
		waktu_transaksi ,
		route_code ,
		route_name ,
		gate_in_boo ,
		count(distinct card_number_var) as jumlah_pelanggan ,
		sum(fare_int) as jumlah_amount
	from staging.dummy_union_transaksi
    where waktu_transaksi::date = '{DATE_FILTER}'
    group by 1, 2, 3, 4;
    """
    pg_hook.run(sql_dummy_agg_by_route.format(DATE_FILTER=ds))
    logging.info("Transformation complete. Created cube.dummy_agg_by_route.")
    
    sql_dummy_agg_by_tarif = """
    delete from cube.dummy_agg_by_tarif WHERE waktu_transaksi::date = '{DATE_FILTER}';
    insert into cube.dummy_agg_by_tarif
    select
	    waktu_transaksi ,
	    fare_int, 
	    gate_in_boo,
	    count(distinct card_number_var) as jumlah_pelanggan ,
		sum(fare_int) as jumlah_amount
	from staging.dummy_union_transaksi
    where waktu_transaksi::date = '{DATE_FILTER}'
    group by 1, 2, 3;
    """
    pg_hook.run(sql_dummy_agg_by_tarif.format(DATE_FILTER=ds))
    logging.info("Transformation complete. Created cube.dummy_agg_by_tarif.")

    logging.info(f"Transformation complete. Aggregated data for {ds} updated in cube schemas.")

@task
def export_to_csv(ds, **kwargs):
    """
    Task 3: Export aggregation tables from cube
    schema to CSV files in the data/output folder.
    """
    logging.info(f"Exporting aggregated data to CSV for date: {ds}...")
    pg_hook = PostgresHook(postgres_conn_id=DWH_CONN_ID)
    
    cube_tables = {
        "dummy_agg_by_card_type": "dummy_agg_by_card_type.csv",
        "dummy_agg_by_route": "dummy_agg_by_route.csv",
        "dummy_agg_by_tarif": "dummy_agg_by_tarif.csv"
    }
    
    # Filename will include the execution date
    date_str = ds.replace('-', '')
    
    for table_name, base_file_name in cube_tables.items():
        file_name = f"{base_file_name}_{date_str}.csv"
        logging.info(f"Exporting cube.{table_name} to {file_name}")
        
        # Get data ONLY for the execution date
        df = pg_hook.get_pandas_df(f"SELECT * FROM cube.{table_name} WHERE waktu_transaksi = '{ds}'")
        df.to_csv(f"{OUTPUT_DIR}/{file_name}", index=False, quoting=csv.QUOTE_NONNUMERIC)
        
    logging.info(f"All CSV exports complete for {ds}.")


@dag(
    dag_id="dag_datapelangan",
    start_date=pendulum.datetime(2025, 7, 1, tz=local_tz),
    schedule="0 7 * * *",  # Runs daily at 07:00
    catchup=False,
    tags=["transjakarta", "take-home-test"],
)
def data_pipeline_dag():
    """
    DAG for Transjakarta Take-Home Test [cite: 1]
    - Extracts data to staging
    - Transforms in PostgreSQL
    - Loads to cube and exports CSVs
    """
    # Define the task dependencies
    init_task = init_schemas()
    load_task = load_to_staging()
    transform_task = transform_in_postgres()
    export_task = export_to_csv()
    
    # Set the pipeline flow: init -> load -> transform -> export 
    init_task >> load_task >> transform_task >> export_task

# This makes the DAG visible to Airflow
data_pipeline_dag()