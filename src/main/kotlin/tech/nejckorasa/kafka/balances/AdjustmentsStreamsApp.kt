package tech.nejckorasa.kafka.balances

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import tech.nejckorasa.kafka.balances.model.AdjustBalance
import tech.nejckorasa.kafka.balances.model.AdjustBalanceSerializer
import java.util.*
import kotlin.random.Random

object AdjustmentsStreamsApp {

    @JvmStatic
    fun main(args: Array<String>) {
        startProducingAdjustBalanceRecords()

        val streams = AdjustmentsStreams()
        streams.cleanUp()
        streams.start()
        Runtime.getRuntime().addShutdownHook(Thread(streams::close))
    }

    /**
     * Produces AdjustBalance record every second
     */
    private fun startProducingAdjustBalanceRecords() {
        Thread {
            KafkaProducer<String, AdjustBalance>(Properties().apply {
                put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS)
                put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
                put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AdjustBalanceSerializer::class.java)
            }).apply {
                while (true) {
                    val ab = AdjustBalance(
                        accountId = "${Random.nextInt(10)}",
                        adjustedAmount = Random.nextLong(-2, 10) * 10
                    )
                    send(ProducerRecord(KafkaConfig.INPUT_TOPIC, ab.accountId, ab)).get()
                    Thread.sleep(1000)
                }
            }
        }.start()
    }
}