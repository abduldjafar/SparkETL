import boto3
import json
import argparse


def spark_emr_steps(spark_packages,path,jar_name):
    Steps = [
        {
            "Name": "etl-to-bronze-delta-lake",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--packages",
                    spark_packages,
                    "s3a://kotekaman-dev/{}{}".format(path,jar_name),
                    "s3://kotekaman-dev/config/application.conf",
                    "s3a://kotekaman-dev/",
                    "s3a://kotekaman-dev/data-sources/",
                ],
            },
        }
    ]
    return Steps

def main(json_config,path,jar_name):

    with open(json_config, "r") as json_file:
        config = json.load(json_file)
    
    connection = boto3.client(
        "emr",
        region_name=config["region_name"],
    )

    spark_packages = config["spark_packages"]
    spark_packages = ",".join(spark_packages)

    cluster_id = connection.run_job_flow(
        Configurations=config["spark_emr_extra_config"],
        Name=config["Name"],
        LogUri=config["LogUri"],
        ReleaseLabel=config["ReleaseLabel"],
        Applications=config["Applications"],
        Instances=config["Instances"],
        Steps=spark_emr_steps(spark_packages,path,jar_name),
        VisibleToAllUsers=config["VisibleToAllUsers"],
        JobFlowRole=config["JobFlowRole"],
        ServiceRole=config["ServiceRole"],
    )

    return cluster_id


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-c", "--config", help="emr json config file", default="emr-config.json"
    )
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

    config = args.config
    file_path = args.path
    filename = args.filename

    cluster_id = main(config,file_path,filename)
    
    print("cluster created with the step...", cluster_id["JobFlowId"])
