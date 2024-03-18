package cc.frankvicky.kafka.wikimedia

import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.EventSource
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import java.net.URI
import java.util.*
import java.util.concurrent.TimeUnit

fun main() {
    val producer = buildKafkaProducer()
    val topicName = "wikimedia.recentchange"
    val eventHandler: EventHandler = WikiMediaChangeHandler(producer, topicName)
    val url = "https://stream.wikimedia.org/v2/stream/recentchange"
    val eventSource = EventSource.Builder(eventHandler, URI.create(url))
        .build()

    eventSource.start()

    TimeUnit.MINUTES.sleep(10)
}

private fun buildKafkaProducer(): KafkaProducer<String, String> {
    val bootStrapServers = "127.0.0.1:9092"

    val properties = Properties().apply {
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers)
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        // set safe producer configs if Kafka <= v2.8
//        setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
//        setProperty(ProducerConfig.ACKS_CONFIG, "all")
//        setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
    }

    return KafkaProducer(properties)
}

