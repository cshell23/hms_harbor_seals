def upload_to_labelbox():
    # 1. Use gsutil to get the URIs from GCS bucket.

    bucket_name = "db-bucket-1"
    folder_name = "gphotos"
    command = f"gsutil ls gs://{bucket_name}/{folder_name}/"

    # Execute the command and get the output
    result = subprocess.run(command.split(), capture_output=True, text=True)
    if result.returncode != 0:
        print("Error executing gsutil command.")
        print(result.stderr)
        exit(1)

    # Get the list of files, excluding .HEIC files
    all_gcs_image_urls = [url for url in result.stdout.split("\n") if url and not url.endswith('.HEIC')]

    # Authenticate with Labelbox.

    lb_client = labelbox.Client()

    # Create a new dataset.

    dataset_name = "hspimages_westbeach_jul26_aug11_2023"
    dataset = lb_client.create_dataset(name=dataset_name)

    # 4. Upload your images to this dataset.
    batch_size = 1000
    test_size = 1558
    for i in range(0, test_size, batch_size):
        # Break down into batch 
        batch = all_gcs_image_urls[i:i+batch_size]

        # Construct a list of data rows
        data_rows = [{'row_data': url} for url in batch]

        #  Upload in a single batch
        dataset.create_data_rows(data_rows)

    print(f"Test dataset {dataset_name} created with images from GCS!")




