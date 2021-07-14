import re
import apache_beam as beam
from apache_beam import pipeline
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.runners import DataflowRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
from apache_beam.io.gcp.internal.clients import bigquery
import logging
import google.auth
import gc

table_schema = 'text:STRING'

table_spec = bigquery.TableReference(
    projectId='transactiondata',
    datasetId='dw_pl_ml_freshdesk',
    tableId='redacted_unlabeled_v3')

source_spec = bigquery.TableReference(
    projectId='transactiondata',
    datasetId='dw_pl_ml_freshdesk',
    tableId='redacted_unlabeled_slim_v2')

out_spec = bigquery.TableReference(
    projectId='transactiondata',
    datasetId='dw_pl_ml_freshdesk',
    tableId='redacted_fully_sample')




def concatenate(x, y, z): 
    return zip(x, y, z)  #[(2, 5, "Person_Name"), ...]
def reverse_sort(iterable):
    my_list = list(iterable)
    my_list.sort(reverse=True)
    return my_list

"""
here is an example of a custom pt-transform and this is probably what we need here as well 
"""

def process(element):
    def reverse_sort(iterable):
        my_list = list(iterable)
        my_list.sort(reverse=True)
        return my_list
    def concatenate(x, y, z): 
        return zip(x, y, z)  #[(2, 5, "Person_Name"), ...]
    
    return (element['conv'], 
            reverse_sort(concatenate(element['start_loc'], element['end_loc'], element['info_type'])))




def RedactAllFn(element):
    text = element[0]
    tup_list = element[1]
    
    def redact(x, y): #(3, 9, "PERSON_NAME")
        x = bytes(x, 'utf-8')
        head=x[: y[0]]
        tail=x[y[1]:]
        # the following try and except clauses 
        # are necessary because if a head or tail 
        # is empty it could otherwise cause problems
        # previously I used str(head) to decode
        # and that actually doesn't decode the byte
        # string and thus the redact_all function 
        # recursively encoded things and never decoded them
        
        try:
            head_string = head.decode('utf-8')
        except:
            head_string = ""
        try:
            tail_string = tail.decode('utf-8')
        except:
            tail_string = ""
            
        return head_string + ' ' + y[2] + ' ' + tail_string
    

    def redact_all(text, tup_list):
        if len(tup_list) > 100:
            text= bytes(text, 'utf-8')[:tup_list[-100][0]].decode('utf-8')
            tup_list = tup_list[-99: ]
            
        if len(tup_list)==1:
        
            return redact(text, tup_list[0])
        else:
            return redact_all(redact(text, tup_list[0]), tup_list[1:])
        
    
    return {'text': redact_all(text, tup_list)}


if __name__=='__main__':
    

    p= pipeline.Pipeline()
    locs = (
  
        p
        | 'ReadTable' >> beam.io.ReadFromBigQuery(table=source_spec, gcs_location='gs://beam-redaction-temp', use_standard_sql=True)
        # Each row is a dictionary where the keys are the BigQuery columns
        | 'preProcess' >> beam.Map(process)
        | 'RedactAll'  >> beam.Map(RedactAllFn) 
        # using ParDo instead of Map caused some problems; use Map if you want to preserve the structure that you decided on!  
        | beam.io.WriteToBigQuery(
            out_spec,
            schema=table_schema,
            custom_gcs_temp_location = 'gs://beam-redaction-temp', 
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))
    
    options = PipelineOptions(
        runner='DataflowRunner',
        project='transactiondata',
        job_name='temp-name',
        temp_location='gs://beam-redaction-temp',
        region='europe-west2',
        machine_type ="e2-highmem-16"
    )
    pipeline_result = DataflowRunner().run_pipeline(p, options=options)
