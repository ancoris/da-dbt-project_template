from google.cloud import storage
from google.cloud import bigquery
import pandas as pd

#hardcode some useful params
project_id='transactiondata'
dataset_id='dw_pl_ml_freshdesk'
table_id='redacted_unlabeled_slim_v2_sample'

## Helper functions
def reverse_sort(list):
    reversed_list = list
    reversed_list.sort(reverse=True)
    return reversed_list

def redact(conv, start_loc, end_loc, info_type):
    x = conv.encode('utf8')
    head=x[:start_loc]
    tail=x[end_loc:]
    try:
        head_str= head.decode("utf-8")
    except:
        head_str = ''

    try:
        tail_str= tail.decode("utf-8")
    except:
        tail_str = ''

    return head_str + ' ' + info_type + ' '+ tail_str


#this is the query that will collect data from bq
query = """
    select *
    from `transactiondata.dw_pl_ml_freshdesk.redacted_unlabeled_slim_v2`  limit 1
"""

# initialise bq client
bigquery_client = bigquery.Client(project = project_id)

#fire query
conversation_data = bigquery_client.query(query).result().to_dataframe()

# initialise the final list
final_redacted_data = []

for i in range(len(conversation_data)):

    #progress update
    if i%100 == 0:
        print('row: {}'.format(i))

    #extract conversation data for this row
    data_for_processing = conversation_data.loc[i]['conv']

    #extract the arrays as lists, more familiar handling!
    start_loc_reversed = reverse_sort(conversation_data.loc[i]['start_loc'].tolist())
    end_loc_reversed = reverse_sort(conversation_data.loc[i]['end_loc'].tolist())
    info_type_reversed = reverse_sort(conversation_data.loc[i]['info_type'].tolist())

    # determine how many loops required for this conversation
    num_redactions = len(start_loc_reversed)

    #this is the main loop that does the redaction
    for redact_index in range(num_redactions):
        data_for_processing = redact(data_for_processing, start_loc_reversed[redact_index], end_loc_reversed[redact_index], info_type_reversed[redact_index])

    # conversation fully processed, append to separate list
    final_redacted_data.append(data_for_processing)




## Upload process
#create df
df = pd.DataFrame({'redacted_conv':final_redacted_data})
print('number of rows in dataframe: {}'.format(len(df)))

i_want_to_upload = True
file_name = 'redacted_conversations_1.json'
bucket_name = 'dw_freshdesk'

if i_want_to_upload:
    def google_storage_authentication(project_id=None):
        try:
            storage_client = storage.Client(project=project_id)
            #print("storage client created")
            return storage_client
        except Exception as e:
            print("Failed to establish Storage Client: {}".format(e))

    storage_client_upload = google_storage_authentication(project_id = project_id)

    def bucket_upload(client, bucket_name, file_name, dataframe, sub_directory_path=''):
        """
        upload of a file to buckets, by default subdirectory is empty
        bucket will need to be specified
        """
        if not sub_directory_path.endswith("/"):
            sub_directory_path += "/"

        bucket = client.get_bucket(bucket_name)
        file_blob = bucket.blob(sub_directory_path + file_name)
        file_blob.upload_from_string(dataframe.to_csv(index=False,encoding='utf-8-sig'), 'text/csv')
        uri = "gs://{}/{}{}".format(bucket_name, sub_directory_path, file_name)
        return uri

    def bucket_upload_json(client, bucket_name, file_name, dataframe, sub_directory_path=''):
        """
        upload of a file to buckets, by default subdirectory is empty
        bucket will need to be specified
        """
        if not sub_directory_path.endswith("/"):
            sub_directory_path += "/"

        bucket = client.get_bucket(bucket_name)
        file_blob = bucket.blob(sub_directory_path + file_name)
        file_blob.upload_from_string(dataframe.to_json(orient='records'), 'text/json')
        uri = "gs://{}/{}{}".format(bucket_name, sub_directory_path, file_name)
        return uri

    #upload time
    try:
        bucket_upload_json(client=storage_client_upload,
                      dataframe=df, file_name=file_name,
                      bucket_name=bucket_name, sub_directory_path="for-labeling-googlers/") #dev/local variation
        print("upload of file name: {} successful".format(file_name))

    except Exception as e:
        print("upload of file name: {} failed with error {}".format(file_name, e))
