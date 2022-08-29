package notification
import java.net.{http, URI}
import com.typesafe.config.{Config => TConfig}


object Notification {
  def toDiscord(
      title: String,
      config: TConfig,
      message: String
  ): Unit = {

    val request = http.HttpRequest
      .newBuilder()
      .uri(
        URI.create(
          config.getString("discord.url")
        )
      )
      .header("Content-Type", "application/json")
      .method(
        "POST",
        http.HttpRequest.BodyPublishers.ofString(
          "{\n    \"embeds\": [\n    {\n      \"title\": \" " + title + " \",\n      \"description\": \"" + message + "\",\n      \"color\": 15258703\n}]}"
        )
      )
      .build();

    val response = http.HttpClient
      .newHttpClient()
      .send(request, http.HttpResponse.BodyHandlers.ofString());
    println(response.statusCode())
    println(response.body())

  }
}
