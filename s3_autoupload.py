import argparse
import boto3
import time

from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler, FileSystemEvent


class S3Uploader(FileSystemEventHandler):
    def __init__(self, bucket: str, url: str, key: str, secret: str, basepath: Path) -> None:
        super().__init__()
        # create an s3 client and pass in the credentials, url and region
        self.client = boto3.client(
            's3', aws_access_key_id=key, aws_secret_access_key=secret, endpoint_url=url, region_name='us-east-1')
        self.bucket = bucket
        self.basepath = basepath
        print("ready to upload files to s3")

    """compare all files in the directory to the files in the bucket and upload any new files, delete any files that are no longer in the directory"""
    def mirror(self):
        print("initial mirroring of files...")
        # get all the files in the directory
        local_files = set()
        for path in self.basepath.glob("**/*"):
            if path.is_file():
                local_files.add(path.relative_to(self.basepath).as_posix())

        files_to_upload = local_files.copy()
        files_to_delete = set()

        # get all the files in the bucket
        file_in_bucket = set()
        response = self.client.list_objects_v2(Bucket=self.bucket)
        if(response['KeyCount'] > 0):
            for obj in response['Contents']:
                file_in_bucket.add(obj['Key'])
            while response['IsTruncated']:
                response = self.client.list_objects_v2(
                    Bucket=self.bucket, ContinuationToken=response['NextContinuationToken'])
                for obj in response['Contents']:
                    file_in_bucket.add(obj['Key'])

        for obj in file_in_bucket:
            if obj not in local_files:
                files_to_delete.add(obj)
            elif obj in files_to_upload:
                files_to_upload.remove(obj)

        # upload any new files
        for file in files_to_upload:
            self.client.upload_file(
                self.basepath.joinpath(file), self.bucket, file)
            print(f"Uploaded {file} to {self.bucket}")

        # delete any files that are no longer in the directory
        for file in files_to_delete:
            self.client.delete_object(Bucket=self.bucket, Key=file)
            print(f"Deleted {file} from {self.bucket}")

        print("initial mirroring complete")

    def on_created(self, event: FileSystemEvent) -> None:
        try:
            filepath = Path(event.src_path).relative_to(
                self.basepath).as_posix()
            self.client.upload_file(
                event.src_path, self.bucket, filepath)
            print(f"Uploaded {event.src_path} to {self.bucket}")
        except Exception as e:
            print(f"Error uploading {event.src_path} to {self.bucket}: {e}")

    def on_deleted(self, event: FileSystemEvent) -> None:
        try:
            filepath = Path(event.src_path).relative_to(
                self.basepath).as_posix()
            self.client.delete_object(Bucket=self.bucket, Key=filepath)
            print(f"Deleted {event.src_path} from {self.bucket}")
        except Exception as e:
            print(f"Error deleting {event.src_path} from {self.bucket}: {e}")

    def on_modified(self, event: FileSystemEvent) -> None:
        self.on_created(event)

    def on_moved(self, event: FileSystemEvent) -> None:
        self.on_deleted(event)
        self.on_created(event)


class Watcher:
    def __init__(self, dir, event_handler) -> None:
        self.observer = Observer()
        self.observer.schedule(event_handler, dir, recursive=True)

    def watch(self):
        """Watch a directory for changed files using watchdog"""
        self.observer.start()
        print("Watching for changes...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.observer.stop()
        self.observer.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Watch a directory for changes and upload to S3")
    parser.add_argument('dir', help="Directory to watch")
    parser.add_argument('bucket', help="S3 bucket name")
    parser.add_argument('url', help="S3 endpoint url")
    parser.add_argument('key', help="S3 access key")
    parser.add_argument('secret', help="S3 secret key")
    args = parser.parse_args()

    path = Path(args.dir)

    # create an instance of the s3_uploader class
    s3handler = S3Uploader(args.bucket, args.url, args.key, args.secret, path)
    s3handler.mirror()

    # TODO remove this
    # create an event handler
    event_handler = LoggingEventHandler()

    w = Watcher(args.dir, s3handler)
    w.watch()
