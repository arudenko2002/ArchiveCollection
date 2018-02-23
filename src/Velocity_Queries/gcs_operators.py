import logging
import time
#silence some annoying warnings
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

#set logging level for the plugin
logging.getLogger(__name__).setLevel(logging.INFO)

from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class WaitGCSOperator(BaseOperator):
    """
    List all objects from the bucket with the give string prefix and delimiter in name.
    This operator returns a python list with the name of objects which can be used by
     `xcom` in the downstream task.
    :param bucket: The Google cloud storage bucket to find the objects.
    :type bucket: string
    :param prefix: Prefix string which filters objects whose name begin with this prefix
    :type prefix: string
    :param delimiter: The delimiter by which you want to filter the objects.
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string
    Example: The following Operator would list all the Avro files from `sales/sales-2017`
        folder in `data` bucket.
    GCS_Files = WaitGCSOperator(
        task_id='GCS_Files',
        bucket=bucket,
        prefix=prefix,
        number="20",
        google_cloud_storage_conn_id="google_cloud_default",
        dag=dagvelocitydaily2
    )
    """
    template_fields = ('bucket', 'prefix', 'number')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix=None,
                 number="1000",
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(WaitGCSOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.number=number
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        service = hook.get_conn()
        answer = False
        counter=0
        while(True):
            pageToken = None
            while(True):
                response = service.objects().list(
                    bucket=self.bucket,
                    pageToken=pageToken,
                    prefix=self.prefix
                ).execute()

                if 'items' not in response:
                    print("No items found for prefix: "+self.prefix)
                    break

                if len(response['items']) > int(self.number):
                    answer=True

                for item in response['items']:
                    if item and 'name' in item:
                        print item['name']

                if 'nextPageToken' not in response:
                    # no further pages of results, so stop the loop
                    break

                pageToken = response['nextPageToken']
                if not pageToken:
                    # empty next page token
                    break


            if answer:
                print ("files exist, move to the next step")
                return
            else:
                print ("files do not exists.  Waiting...")
                time.sleep(120)

            counter=counter+1
            if counter>30:
                print ("Files were not created after 1 hour.  Tomeouting...")
                return

class BigQueryPlugin(AirflowPlugin):
    name = "GCS Plugin"
    operators = [
        WaitGCSOperator
    ]

