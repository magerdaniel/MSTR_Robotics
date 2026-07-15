import time

from azure.storage.blob import BlobServiceClient


class MsAzure:
    def delete_mmp_files_in_folder(self, connection_string, container_name, folder_path):
        """
        Delete all .mmp files directly under a specific folder path in Azure Blob Storage.
        Does NOT delete files in subfolders.

        Args:
            connection_string: Azure storage connection string
            container_name: Container name (e.g., 'mstrmigrations')
            folder_path: Folder path (e.g., 'mstr/shared/migrations/packages/2025-12-16')

        Returns:
            dict with 'deleted_count', 'deleted_files', and 'errors' keys
        """
        try:
            # Create blob service client
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container_name)

            # Ensure folder path ends with /
            if not folder_path.endswith('/'):
                folder_path = folder_path + '/'

            deleted_files = []
            errors = []

            # List all blobs with the folder prefix
            blob_list = container_client.list_blobs(name_starts_with=folder_path)

            for blob in blob_list:
                # Check if file is directly in the folder (not in subfolders)
                relative_path = blob.name[len(folder_path):]

                # Skip if it's in a subfolder (contains '/')
                if '/' in relative_path:
                    continue

                # Check if it's an .mmp file
                if blob.name.endswith('.mmp'):
                    try:
                        blob_client = blob_service_client.get_blob_client(
                            container=container_name,
                            blob=blob.name
                        )
                        blob_client.delete_blob()
                        deleted_files.append(blob.name)
                        # print(f"✓ Deleted: {blob.name}")
                    except Exception as err:
                        errors.append({'file': blob.name, 'error': str(err)})
                        # print(f"✗ Failed to delete {blob.name}: {err}")

            return {
                'success': True,
                'deleted_count': len(deleted_files),
                'deleted_files': deleted_files,
                'errors': errors
            }

        except Exception as err:
            return {
                'success': False,
                'error': str(err)
            }

    def move_blob_to_folder(self, connection_string, container_name, source_path, target_path):
        """
        Move a blob from one location to another within the same container.
        Creates target folder in the same parent directory as the source file.
        Deletes the source blob after successful copy.
        """
        try:
            # Create blob service client
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)

            source_blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=source_path
            )

            # Get target blob client
            target_blob_client = blob_service_client.get_blob_client(
                container=container_name,
                blob=target_path
            )

            # Copy blob (async operation)
            source_url = source_blob_client.url
            copy_operation = target_blob_client.start_copy_from_url(source_url)

            # Wait for copy to complete
            properties = target_blob_client.get_blob_properties()
            while properties.copy.status == 'pending':
                time.sleep(1)
                properties = target_blob_client.get_blob_properties()

            # Check if copy was successful
            if properties.copy.status == 'success':
                # Delete source blob after successful copy
                source_blob_client.delete_blob()

                return {
                    'success': True,
                    'source_path': source_path,
                    'target_path': target_path,
                    'operation': 'moved',
                    'copy_operation': copy_operation
                }
            else:
                return {
                    'success': False,
                    'source_path': source_path,
                    'error': f'Copy failed with status: {properties.copy.status}',
                    'copy_operation': copy_operation
                }

        except Exception as err:
            return {
                'success': False,
                'source_path': source_path,
                'error': str(err),
                'copy_operation': copy_operation
            }
