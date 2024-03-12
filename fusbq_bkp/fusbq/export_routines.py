
from google.cloud import bigquery
import os
import sqlparse

data_project_name = 'helix-data-dev'
path = 'C:\\Users\\VinodKumarM\\Desktop\\SIL_PROCS'

client = bigquery.Client(data_project_name)
sql = 'select specific_catalog, specific_schema, specific_name, ddl from  ' \
      'helix-data-dev.helix_dw.INFORMATION_SCHEMA.ROUTINES where specific_name like "%_AR_%"'

print(sql)

df = client.query(sql).to_dataframe()
dicts = df.to_dict(orient='records')

for a in dicts:
    with open(os.path.join(path, a['specific_name'] + '.sql'), 'w') as file:
        file.write(sqlparse.format(a['ddl'], reindent=True, keyword_case='upper'))
