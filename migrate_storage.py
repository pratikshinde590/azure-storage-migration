from azure.storage.blob import BlobServiceClient, ContainerClient
import os

# Connection strings for source and destination storage accounts
SOURCE_CONNECTION_STRING = "ENTER_SOURCE_CONNECTION_STRING"
DESTINATION_CONNECTION_STRING = "ENTER_DESTINATION_CONNECTION_STRING"

# Source and destination container names
SOURCE_CONTAINER = "ENTER_SOURCE_CONTAINER_NAME"
DESTINATION_CONTAINER = "ENTER_DESTINATION_CONTAINER_NAME"

def migrate_blobs():
    # Connect to source and destination blob services
    source_blob_service = BlobServiceClient.from_connection_string(SOURCE_CONNECTION_STRING)
    dest_blob_service = BlobServiceClient.from_connection_string(DESTINATION_CONNECTION_STRING)

    # Access source and destination containers
    source_container_client = source_blob_service.get_container_client(SOURCE_CONTAINER)
    dest_container_client = dest_blob_service.get_container_client(DESTINATION_CONTAINER)

    # Create the destination container if it does not exist
    if not dest_container_client.exists():
        dest_container_client.create_container()

    print("Starting blob migration...")

    # List blobs in the source container
    blobs = source_container_client.list_blobs()

    for blob in blobs:
        print(f"Copying blob: {blob.name}")

        # Get source blob URL with SAS token for secure access
        source_blob_url = source_container_client.url + "/" + blob.name

        # Copy blob to destination
        dest_blob_client = dest_container_client.get_blob_client(blob.name)
        dest_blob_client.start_copy_from_url(source_blob_url)

    print("Migration completed successfully!")

if __name__ == "__main__":
    migrate_blobs()