import csv
import re
from google.cloud import storage
from google.cloud import bigquery
client = bigquery.Client()
bucket_client = storage.Client()

bucket = bucket_client.get_bucket('dw_freshdesk')
location = bucket.location

from google.cloud import bigquery_storage
bqstorageclient = bigquery_storage.BigQueryReadClient()
bqclient = bigquery.Client()

def query_to_jsonl(table, bucket_uri):

    query_string = """
    select
    concat('sample/', concat(cast(id as string), ".txt")) as text_id, 
    replace(replace(body_text, ",", ""), "-", "") as body_text, 
    concat('{}/', concat('sample/', concat(cast(id as string), '.txt'))) as content, 
    "text/plain" as mimeType
    from `transactiondata.dw_pl_ml_freshdesk.{}`
    """.format(bucket_uri, table)

    sample = (
        bqclient.query(query_string)
        .result()
        .to_dataframe(bqstorage_client=bqstorageclient)
    )


    for iD, text in zip(list(sample['text_id']), list(sample['body_text'])):
        blob = storage.Blob(iD, bucket)
        blob.upload_from_string(text)
    
    blob2 = storage.Blob("sample.jsonl", bucket)

    s = sample[['content', 'mimeType']].to_json(orient='records', lines=True)
    t = re.sub('[^\u0000-\u007f]', '',  s)

    blob2.upload_from_string(t)

def main():
    
    table='sample2'
    bucket_uri = 'gs://'+ bucket.name
    query_to_jsonl(table, bucket_uri)
    
if __name__=='__main__':
    main()