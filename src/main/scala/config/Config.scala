package config

import java.io.File

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3URI}
import com.typesafe.config.{ConfigFactory, Config => TConfig}

import scala.io.Source

object Config {

  private def read(location: String): String = {
    val awsCredentials = new DefaultAWSCredentialsProviderChain()
    val s3Client = new AmazonS3Client(awsCredentials)
    val s3Uri = new AmazonS3URI(location)

    val fullObject = s3Client.getObject(s3Uri.getBucket, s3Uri.getKey)

    Source.fromInputStream(fullObject.getObjectContent).getLines.mkString("\n")
  }

  def apply(location: String): TConfig = {

    if (location.startsWith("s3")) {
      val content = read(location)
      ConfigFactory.parseString(content)
    } else {
      ConfigFactory.parseFile(new File(location))
    }
  }
}