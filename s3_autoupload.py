import argparse
import logging
import boto3
import time

from pathlib import Path
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer
from watchdog.events import FileSystemEvent


class S3Uploader(FileSystemEventHandler):
    def __init__(self, bucket: str, url: str, key: str, secret: str, basepath: Path) -> None:
        super().__init__()
        # create an s3 client and pass in the credentials, url and region
        self.client = boto3.client(
            's3',
            aws_access_key_id=key,
            aws_secret_access_key=secret,
            endpoint_url=url,
            region_name='us-east-1'
        )
        self.bucket = bucket
        self.basepath = basepath
        logging.info("ready to upload files to s3")

    def mirror(self):
        """
        compare all files in the directory to the files in the bucket
        upload any new files, delete any files that are no longer in the directory
        """
        logging.info("initial mirroring of files...")
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
        if response['KeyCount'] > 0:
            for obj in response['Contents']:
                file_in_bucket.add(obj['Key'])

            while response['IsTruncated']:
                response = self.client.list_objects_v2(
                    Bucket=self.bucket,
                    ContinuationToken=response['NextContinuationToken'],
                )

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
                self.basepath.joinpath(file),
                self.bucket,
                file,
            )
            logging.info(f"Uploaded {file} to {self.bucket}")

        # delete any files that are no longer in the directory
        for file in files_to_delete:
            self.client.delete_object(Bucket=self.bucket, Key=file)
            logging.info(f"Deleted {file} from {self.bucket}")

        logging.info("initial mirroring complete")

    def upload(self, filepath: Path, count=1) -> None:
        """Upload a single file to the bucket"""
        try:
            self.client.upload_file(
                filepath,
                self.bucket,
                filepath.relative_to(self.basepath).as_posix()
            )
            logging.info(f"Uploaded {filepath} to {self.bucket}")

        except PermissionError as e:
            if(count > 5):
                logging.error(f"{filepath} could not be uploaded: {e.with_traceback(None)}")
                return
            logging.error(f"{filepath} could not be uploaded: {e}\nwaiting {count*5} seconds and trying again...")
            time.sleep(5*count)
            self.upload(filepath, count + 1)

    def on_created(self, event: FileSystemEvent) -> None:
        filepath = Path(event.src_path)
        if not filepath.exists() or not filepath.is_file():
            return

        self.upload(filepath)

    def on_deleted(self, event: FileSystemEvent) -> None:
        filepath = Path(event.src_path).relative_to(self.basepath).as_posix()
        self.client.delete_object(Bucket=self.bucket, Key=filepath)
        logging.info(f"Deleted {event.src_path} from {self.bucket}")

    def on_modified(self, event: FileSystemEvent) -> None:
        self.on_created(event)

    def on_moved(self, event: FileSystemEvent) -> None:
        filepath = Path(event.dest_path)
        if not filepath.exists() or not filepath.is_file():
            return
        self.upload(filepath)
        self.on_deleted(event)


class Watcher:
    def __init__(self, dir, event_handler) -> None:
        self.observer = Observer()
        self.observer.schedule(event_handler, dir, recursive=True)

    def watch(self):
        """Watch a directory for changed files using watchdog"""
        self.observer.start()
        logging.info("Watching for changes...")
        try:
            while True:
                time.sleep(1)

        except KeyboardInterrupt:
            self.observer.stop()

        self.observer.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Watch a directory for changes and upload to S3")
    parser.add_argument('dir', help="Directory to watch")
    parser.add_argument('bucket', help="S3 bucket name")
    parser.add_argument('url', help="S3 endpoint url")
    parser.add_argument('key', help="S3 access key")
    parser.add_argument('secret', help="S3 secret key")
    parser.add_argument('--log', help="Log file path", default="s3_autoupload.log")
    args = parser.parse_args()

    path = Path(args.dir)

    logging.basicConfig(
        filename=args.log,
        level=logging.INFO,
        filemode="a+",
        format="%(asctime)-15s %(levelname)-8s %(message)s",
    )

    s3handler = S3Uploader(args.bucket, args.url, args.key, args.secret, path)
    s3handler.mirror()

    w = Watcher(args.dir, s3handler)
    w.watch()
