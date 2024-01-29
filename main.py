import labelbox
from google.cloud import secretmanager
from google.cloud import storage
import datetime
import pytz

def get_secret(secret_name):
    client = secretmanager.SecretManagerServiceClient()
    secret_version = f"projects/35596334323/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_version})
    return response.payload.data.decode('UTF-8')

def list_blobs(bucket_name, folder_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=folder_name)

    all_gcs_image_data = []

    for blob in blobs:
        if blob.content_type == 'image/jpeg':
            metadata = blob.metadata
            if metadata is not None and "goog-reserved-file-mtime" in metadata:
                timestamp_utc = int(metadata["goog-reserved-file-mtime"])
                dt_utc = datetime.datetime.utcfromtimestamp(timestamp_utc).replace(tzinfo=pytz.utc)
                dt_pdt = dt_utc.astimezone(pytz.timezone('US/Pacific'))
                metadata["local_time"] = dt_pdt.strftime('%Y-%m-%dT%H:%M:%SZ')

            all_gcs_image_data.append({
                "url": f"gs://{bucket_name}/{blob.name}",
                "metadata": metadata
            })

    return all_gcs_image_data


def upload_to_labelbox(request):
    bucket_name = "db-bucket-1"
    folder_name = "gphotos"
    location = "west_beach"
    api_key = get_secret("lb_api_key")
    all_gcs_image_data = list_blobs(bucket_name, folder_name)

    lb_client = labelbox.Client(api_key)

    dataset_name = "hsp_west_beach_jul26_aug11_2023"
    dataset = lb_client.create_dataset(name=dataset_name)

    batch_size = 500
    for i in range(0, len(all_gcs_image_data), batch_size):
        batch = all_gcs_image_data[i:i+batch_size]
        data_rows = []
        for item in batch:
            row_data = {'row_data': item["url"],
                        'external_id': item["metadata"]["local_time"]}
            
            # Creating the metadata fields
            # Location metadata
            tag_metadata_field = {
                "name": "tag",
                "value": location,
            }

            # Construct an metadata field of datetime
            datetime_metadata_field = {
                "name": "captureDateTime",
                "value": item["metadata"]["local_time"],
            }
            row_data['metadata_fields'] = [tag_metadata_field, datetime_metadata_field]
            data_rows.append(row_data)

        dataset.create_data_rows(data_rows)
    return f"Dataset {dataset_name} created with images from GCS!"

