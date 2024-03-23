package cc.frankvicky.kafka.opensearch

import com.google.gson.JsonParser
import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.opensearch.action.bulk.BulkRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestClient
import org.opensearch.client.RestHighLevelClient
import org.opensearch.client.indices.CreateIndexRequest
import org.opensearch.client.indices.GetIndexRequest
import org.opensearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.net.URI
import java.time.Duration
import java.util.*

class OpenSearchConsumer

fun main() {

    val log = LoggerFactory.getLogger(OpenSearchConsumer::class.java.simpleName)
    // create an OpenSearch Client
    val openSearchClient = createOpenSearchClient()

    // create our Kafka Client
    val consumer = createKafkaConsumer().also { it.subscribe(listOf("wikimedia.recentchange")) }

    try {

        openSearchClient.use { client ->
            val indexExisted = client.indices()
                .exists(GetIndexRequest("wikimedia"), RequestOptions.DEFAULT)

            if (!indexExisted) {
                client.indices().create(CreateIndexRequest("wikimedia"), RequestOptions.DEFAULT)
                log.info("hello")
            } else log.info("Index already exists")

            consumer.use { consumer ->
                while (true) {
                    val records = consumer.poll(Duration.ofMillis(3000))
                    log.info("Received ${records.count()} record(s)")

                    val bulkRequest = BulkRequest()

                    records.forEach { record ->
                        IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(extractId(record.value()))
                            .let { bulkRequest.add(it) }
                    }

                    if (bulkRequest.numberOfActions() > 0) {
                        val bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT)
                        log.info("Inserted ${bulkResponse.items.size} record(s)")

                        Thread.sleep(1000)

                        consumer.commitSync()
                        log.info("Offsets have been committed")
                    }
                }
            }
        }
    } catch (exception : WakeupException) {
        log.info("Consumer is starting to shut down")
    } finally {
        consumer.close() // close the consumer, this will also commit offsets
        openSearchClient.close()
        log.info("The consumer is now gracefully shut down")
    }
}

private fun createOpenSearchClient(): RestHighLevelClient {
    val connectionString = "http://localhost:9200"
    val connectionUri = URI.create(connectionString)
    return with(connectionUri) {
        if (userInfo != null)
            buildRestHighLevelClientWithUserInfo()
        else
            RestHighLevelClient(RestClient.builder(HttpHost(host, port, "http")))
    }
}

private fun URI.buildRestHighLevelClientWithUserInfo(): RestHighLevelClient {
    val auth = userInfo.split(":")
    val credentialsProvider = BasicCredentialsProvider().apply {
        setCredentials(AuthScope.ANY, UsernamePasswordCredentials(auth[0], auth[1]))
    }

    return RestClient.builder(HttpHost(host, port, scheme))
        .setHttpClientConfigCallback { builder ->
            builder.setDefaultCredentialsProvider(credentialsProvider)
                .setKeepAliveStrategy(DefaultConnectionKeepAliveStrategy())
        }
        .let { restClientBuilder -> RestHighLevelClient(restClientBuilder) }
}

private fun createKafkaConsumer(): KafkaConsumer<String, String> {
    val bootstrapServers = "127.0.0.1:9092"
    val groupId = "consumer-opensearch-demo"

    // create consumer configs
    val properties = Properties().apply {
        setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
        setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    }

    // create consumer
    return KafkaConsumer(properties)
}


private fun extractId(json: String): String =
    JsonParser.parseString(json)
        .asJsonObject
        .get("meta")
        .asJsonObject
        .get("id")
        .asString
