from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

from airflow.sdk import task
from airflow.sdk import dag
from airflow.models.param import Param
from airflow.sdk import get_current_context

import pendulum

import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage
from google.api_core.exceptions import NotFound, Forbidden
import time



from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType,DoubleType, TimestampType
from pyspark.sql import functions as F

import os
from pathlib import Path
current_dir = Path(os.getcwd())


KEY_PATH = current_dir.parent.parent / "include" /"airflow-gcs-packer.json"

appName = "Read parquet raw file from GCS"
master = "local[*]"

def verify_gcs_upload(client,BUCKET_NAME,blob_name):
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    return blob.exists()

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
#from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping
from cosmos import DbtTaskGroup
from cosmos.config import ProfileConfig
# 定義 dbt 執行設定
# 容器內的路徑：

DBT_PROJECT_PATH = current_dir.parent.parent / "include" / "ny_taxi"


PROJECT_ID= "airflow-workflow-494805" # 根據Google Cloud Platform project_id
DATASET_NAME = "taxi_trips"

project_config = ProjectConfig(dbt_project_path=DBT_PROJECT_PATH)

# Cosmos 會自動抓取 Airflow 的 Connection
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default", # 你在 Airflow 設定的 GCP 連線
        profile_args={"project":PROJECT_ID , "dataset": DATASET_NAME},
    ),
)

        

render_config = RenderConfig(
    # 不寫 select，Cosmos 預設會把 models/ 資料夾下所有的 .sql 都跑一遍
    test_behavior="after_each" 
)



BUCKET_NAME ="nyc_taxi_20-23"
CREDENTIALS_FILE = "airflow-gcs-packer.json"
CHUNK_SIZE = 8 * 1024 * 1024
PROJECT_ID= "airflow-workflow-494805" 
DATASET_NAME = "taxi_trips"
DOWNLOAD_TO_DIR = "raw_data"




@dag(
    dag_id='taxi_monthly_manual',
    start_date=datetime(2023, 1, 1),
    schedule='@monthly',  # <-- 平時每個月自動執行，完全不用手動
    catchup=False,
    params={
        "taxi_types": Param(
            ["green", "yellow"], # <-- 這是關鍵！自動跑時會「全跑」這個預設清單
            type="array",
            items={"type": "string", "enum": ["green", "yellow"]},
            description="if need to use backfill, it can choose service type"
        )
    }
)

def workflow():
    
       
        
    @task
    def fetch_data(download_to_dir: str,taxi_type: str):
        
        context = get_current_context()
        logical_date = context["logical_date"]
        year = logical_date.year
        month = logical_date.month
        os.makedirs(download_to_dir, exist_ok=True) # 確保 data 資料夾存在
        url =f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        file_name = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        file_path = os.path.join(download_to_dir, file_name)
        
        try:
            print(f"Downloading{file_name}")
            print(f"URL: {url}")
            urllib.request.urlretrieve(url, file_path) # 像是右鍵 -> 另存新檔
            return file_path
        except Exception as e: # 這裡會告知出錯原因
            print(f"Failed to download {file_name}: {e}")
            raise # 拋出錯誤讓 Airflow 知道任務失敗
        
    
    @task
    def upload_to_gcs(file_path, bucket_name: str):
        
        # check bucket exists :
       
        client = storage.Client.from_service_account_json(KEY_PATH)
        
        try:
            # 1: make sure have bucket in google
            bucket = client.get_bucket(bucket_name)
            # 2 : check the bucket whether exist in my project or not
            project_bucket_ids = [bckt.id for bckt in client.list_buckets()]
            if bucket_name in project_bucket_ids:
                print(f"Bucket {bucket_name} exists in my project.")
            else:
                print(f"Bucket {bucket_name} does not exist in my project.")
                raise Exception("Bucket not found in current project.") # something wrong and exit the program
        # 3: if bucket do not exists, Create One
        except NotFound:
            bucket = client.create_bucket(bucket_name)
            print(f"Create Bucket '{bucket_name}' successfully.")
        except Forbidden:
            print(f"Bucket {bucket_name} is exit, but not accessible.")
            raise PermissionError(f"No permission to access bucket {bucket_name}") # something wrong and exit the program
        
        # upload data to gcs :
        # bytes to KB (1024), KB to MB (1024)
        # recommand : 5MB ~10MB, but not more than 100MB
        # blob : binary large object
        blob_name = os.path.basename(file_path) # only_filename 
        blob = bucket.blob(blob_name) # create a blob object，order a place
        blob.chunk_size = CHUNK_SIZE
        try :
            print(f"Uploading {blob_name} to {bucket_name} : ")
            blob.upload_from_filename(file_path)
            print(f"Upload successful: {blob_name} to gs {bucket_name}")
            if verify_gcs_upload(client,bucket_name,blob_name):
                print(f"Verification successful for {blob_name}")
                return f"gs://{bucket_name}/{blob_name}"

            else:
                print(f"Verification failed for {blob_name}")
        except Exception as e:
            print(f"Failed to upload {blob_name} to gs {bucket_name}: {e}")
            raise e

    
    @task
    def filter_data(gcs_file_path: str):
        # only when use this task will import spark 
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        import os
        from datetime import datetime
        import re
        
        # 初始化 Spark
        appName = "Read parquet raw file from GCS"
        master = "local[*]"
        spark = SparkSession.builder\
            .appName(appName) \
            .master(master) \
            .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.5-shaded.jar") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEY_PATH) \
            .config("spark.sql.repl.eagerEval.enabled", True) \
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
            .getOrCreate()
        
        context = get_current_context()
        logical_date = context["logical_date"]
        year = logical_date.year
        month = logical_date.month
        
        start_date = datetime(year, month, 1, 0, 0, 0)
        if month == 12:
            end_date = datetime(year + 1, 1, 1, 0, 0, 0)
        else:
            end_date = datetime(year, month + 1, 1, 0, 0, 0)
        print(f"data time range：{start_date} to {end_date}")
        
        
        print(gcs_file_path )
        taxi_type_pattern = r"/([^/]+)_tripdata"
        taxi_type= re.search(taxi_type_pattern, gcs_file_path).group(1)
    
       
            
        df=spark.read.parquet(gcs_file_path)
        # 根據計程車類型決定欄位
        
        if 'lpep_pickup_datetime' in df.columns:
            pickup_col = 'lpep_pickup_datetime'
        else:
            pickup_col = 'tpep_pickup_datetime'
            
        if 'lpep_dropoff_datetime'in df.columns:
            dropoff_col = 'lpep_dropoff_datetime'
        else:
            dropoff_col = 'tpep_dropoff_datetime'

        
        df_filter = df.filter(
                    (F.col(pickup_col) >= start_date) & 
                    (F.col(pickup_col) < end_date)
                )
        
        df_filter = df_filter.withColumnRenamed('VendorID', 'vendor_id') \
           .withColumnRenamed(pickup_col, 'pickup_datetime') \
           .withColumnRenamed(dropoff_col, 'dropoff_datetime') \
           .withColumnRenamed('PULocationID', 'pickup_location_id') \
           .withColumnRenamed('DOLocationID', 'dropoff_location_id') \
           .withColumnRenamed('extra', 'surcharge_amount') \
           .withColumnRenamed('tip_amount', 'creditcard_tip_amount')\
           .withColumn("taxi_type", F.lit(taxi_type)) \
           .withColumn("year", F.lit(year)).withColumn("month", F.lit(month))\
          
        
        if taxi_type == 'green':
           df_filter = df_filter.withColumn("ehail_fee", F.col("ehail_fee").cast("double"))
           
           
        row_count = df_filter.count()
        print(f"Completed {taxi_type} data, {row_count} rows")
        
        filtered_gcs_path=(
            f"gs://{BUCKET_NAME}/silver/{taxi_type}/"
           )
        
        if row_count > 0:
            print(f"Writing {row_count} rows to {filtered_gcs_path}")
            # 修正: overwriter -> overwrite
            df_filter.write \
                     .mode("overwrite")\
                     .partitionBy("year", "month") \
                     .parquet(filtered_gcs_path)
        else:
            print(f"No data found for {taxi_type} in interval {start_date} to {end_date}")

        spark.stop()
             
    
   
    @task
    def create_external_table(taxi_type:str):
        from google.cloud import bigquery as bq
        from google.cloud import storage
        
    
        dataset_id = f"{PROJECT_ID}.{DATASET_NAME}" 
        bq_client = bq.Client.from_service_account_json(KEY_PATH)
        
        
  
        table_name = f"ext_{taxi_type}_taxi" # 建議用固定名稱，不要加年份月份
        table_id = f"{PROJECT_ID}.{DATASET_NAME}.{table_name}"
       
       # 3. 外部表配置 (啟用 Hive Partitioning)
        external_config = bq.ExternalConfig("PARQUET")
        # 指向根目錄：
        external_config.source_uris = [ 
            f"gs://{BUCKET_NAME}/silver/{taxi_type}/*"
            ]


        # 設定分區偵測：告訴 BigQuery 路徑裡有partition 分區
        hive_options = bq.HivePartitioningOptions()

        hive_options.mode = "AUTO"
        hive_options.source_uri_prefix = f"gs://{BUCKET_NAME}/silver/{taxi_type}/" # 根目錄前綴，從這裡開始掃瞄尋找

        external_config.hive_partitioning = hive_options
        external_config.autodetect = True
      
        
        
 
        try:
            bq_client.create_dataset(dataset_id, exists_ok=True)
            print(f"✅ Dataset {dataset_id} 已準備就緒（新建或已存在）")
        except Exception as e:
            print(f"❌ 建立失敗: {e}")
            raise

        # 2. 先嘗試刪除舊表 (not_found_ok=True 確保表不存在時不會噴錯)
        bq_client.delete_table(table_id, not_found_ok=True)
    

        # 建立外部表物件
        
        table = bq.Table(table_id)
        table.external_data_configuration = external_config

        # 執行建立
        created_table=bq_client.create_table(table)
        print(f"🆕 External Table {table_id}  created successfully ")
        
        
        print("SOURCE URIS:", table.external_data_configuration.source_uris)
        print("HIVE OPTIONS:", table.external_data_configuration.hive_partitioning)

  

        print("CREATED EXTERNAL CONFIG:", created_table.external_data_configuration)
        print("CREATED SCHEMA:")
        for field in created_table.schema:
            print(field.name, field.field_type)
    
    
    
    @task.branch
    def choose_taxi_branch():
        context = get_current_context()
        taxi_types = context["params"].get("taxi_types", ["green", "yellow"])

        branches = []

        if "green" in taxi_types:
            branches.append("dbt_green")
        if "yellow" in taxi_types:
            branches.append("dbt_yellow")

        return branches
   
   
   
    
    dbt_taxi_pipeline = DbtTaskGroup(
    group_id="dbt_taxi_processing",
    project_config=project_config,
    profile_config=profile_config,
    operator_args={
        "vars": {
            "target_year": "{{ logical_date.strftime('%Y') }}",
            "target_month": "{{ logical_date.strftime('%m') }}"
        }
    },
    render_config=RenderConfig(
        # 同時選擇綠、黃標籤以及它們的下游（+ 號代表包含下游）
    #    select=["tag:taxi"]
    )
    )

# 確保所有外部表都建好後，才開始跑 dbt 的 Staging 和 Join

    # --- 3. 串接任務 (The Dependency Map) ---
    # 獲取 Runtime 參數
    
    # 使用 .expand 來處理動態的 taxi_type
    # 使用 [DOWNLOAD_TO_DIR] 將單一變數包成 list，讓它配合 expand 運作
    # 或者更專業的做法是：使用 partial (固定值)
    
    @task
    def get_taxi_types():
        context = get_current_context()
        return context["params"].get("taxi_types", ["green", "yellow"])
    
    taxi_types = get_taxi_types()
    local_file_paths = fetch_data.partial(download_to_dir=DOWNLOAD_TO_DIR).expand(
    taxi_type=taxi_types
     )
    gcs_file_paths = upload_to_gcs.partial(bucket_name=BUCKET_NAME).expand(
    file_path=local_file_paths
    )
    
    
    cleansed_gcs_paths = filter_data.expand(
    gcs_file_path=gcs_file_paths
    )
    
    
    # 5. 建立外部表 (動態映射)
    external_tables = create_external_table.expand(
        taxi_type=taxi_types
    )
    cleansed_gcs_paths >> external_tables
    
    external_tables >> dbt_taxi_pipeline
    
# Instantiate the DAG
workflow()    