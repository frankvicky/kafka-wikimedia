package cc.frankvicky.kafka.wikimedia

import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.MessageEvent
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class WikiMediaChangeHandler(
    private val producer: KafkaProducer<String, String>,
    private val topic: String
) : EventHandler {

    override fun onOpen() {
        doNothing()
    }

    override fun onClosed() {
        producer.close()
    }

    override fun onMessage(s: String, messageEvent: MessageEvent) {
        log.info(messageEvent.data)
        // async send
        producer.send(ProducerRecord(topic, messageEvent.data))
    }

    override fun onComment(s: String) {
        doNothing()
    }

    override fun onError(throwable: Throwable) {
        log.error("Error in Stream Reading", throwable)
    }

    private fun doNothing() {}

    companion object {
        private val log: Logger = LoggerFactory.getLogger(WikiMediaChangeHandler::class.java.simpleName)
    }
}
