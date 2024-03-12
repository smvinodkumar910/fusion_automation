import pandas as pd
from fusbq.path_config import DataSets
pc = DataSets('common')
import os
from google.cloud import storage
import sqlparse



def create_raw_views(df: pd.DataFrame, raw_table_name, target_path, module):
    pc = DataSets(module)
    raw_view_name = raw_table_name

    raw_meta_cols = "_metadata_insert_time,\n" \
                    "_metadata_update_time,\n" \
                    "_metadata_deleted,\n" \
                    "_metadata_dag_run_id,\n"

    df.to_csv(os.path.join(target_path, 'RAW_VIEWS', 'df.csv'))
    df['view_columns'] = df['raw_column_name'] + ' as ' + df['raw_view_columns'] + ',\n'
    collist = list(df['view_columns'].drop_duplicates())
    columns_str = ''.join(collist)
    columns_str = columns_str + raw_meta_cols
    columns_str = columns_str[:-2] + '\n'
    view_template = "CREATE OR REPLACE VIEW `project_name.{dataset_name}.{view_name}` \nOPTIONS (labels=[label_1]) \nas \n" \
                    "SELECT " \
                    "{columns} " \
                    "from `project_name.{raw_dataset_name}.{raw_table_name}`;".format(dataset_name=pc.VIEW_SCHEMA ,
                                                                                raw_table_name=raw_table_name,
                                                                                view_name=raw_view_name,
                                                                                columns=columns_str, raw_dataset_name = pc.RAW_SCHEMA)

    view_template = sqlparse.format(view_template, reindent=True, keyword_case='upper')
    with open(os.path.join(target_path, 'RAW_VIEWS', raw_view_name + '.sql'), 'w') as file:
        file.write(view_template)

    client = storage.Client(pc.PROJECT_NAME)
    bucket = client.get_bucket(pc.BUCKET_NAME)
    blob = bucket.blob(pc.TEMP_PATH + 'RAW_VIEWS/' + raw_view_name + '.sql')
    blob.upload_from_string(view_template)
    return True
