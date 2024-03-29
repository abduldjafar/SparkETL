import logging
import boto3
from botocore.exceptions import ClientError
import os
import argparse


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client("s3")
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Adding optional argument
    parser.add_argument(
        "-j",
        "--jar",
        help="jar file for upload",
        default="target/scala-2.12/sparketl_2.12-0.1.jar",
    )
    parser.add_argument("-b", "--bucket", help="s3 bucket", default="kotekaman-dev")
    parser.add_argument(
        "-p",
        "--path",
        help="path for save jar in bucket",
        default="spark-applications/",
    )
    parser.add_argument(
        "-f", "--filename", help="filename in bucket", default="sparketl_2.12-0.1.jar"
    )

    # Read arguments from command line
    args = parser.parse_args()

    file = args.jar
    bucket = args.bucket
    path = args.path
    filename = args.filename

    upload_process = upload_file(file, bucket, "{}{}".format(path, filename))
    print(upload_process)
