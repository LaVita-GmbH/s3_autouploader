# S3 Autouploader

## What is this?
This script is a simple way to sync a local folder to an s3 bucket.

It offers a similar functionality to the `mc mirror --watch` command, but with a key difference:  
It can be used to watch smb shares, mc will throw an error when trying to upload a file that is still being locked by the smb process.  
The mc mirror would ignore a file after an error, this script will try to upload it again.

## How to use
1. Clone the repository
2. Install the requirements:  
`pip install -r requirements.txt`
1. Run the script with your credentials:  
`python3 s3_autouploader.py <path-to-watch> <bucket-name> <s3-url> <access-key-id> <access-secret> [--log <log-file>]`  
if you omit the log file, the log will be written to `./s3_autouploader.log`

## License
This project is licensed under the GPL-3.0 License - see the [LICENSE](LICENSE) file for details  
It is provided as is, without any warranty.
