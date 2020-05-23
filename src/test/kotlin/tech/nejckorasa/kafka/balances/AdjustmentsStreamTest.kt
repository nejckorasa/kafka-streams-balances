package tech.nejckorasa.kafka.balances

import tech.nejckorasa.kafka.balances.AdjustmentsStreams.buildTopology
import tech.nejckorasa.kafka.balances.KafkaConfig.INPUT_TOPIC
import tech.nejckorasa.kafka.balances.KafkaConfig.OUTPUT_TOPIC
import tech.nejckorasa.kafka.balances.model.JsonSerde.jsonSerde
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG
import org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.streams.TopologyTestDriver
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS
import tech.nejckorasa.kafka.balances.model.AdjustBalance
import tech.nejckorasa.kafka.balances.model.BalanceAdjusted
import java.util.*

@TestInstance(PER_CLASS)
class BalancesStreamTest {

    private lateinit var factory: ConsumerRecordFactory<String, AdjustBalance>
    private lateinit var topology: TopologyTestDriver

    private val sourceSerde = jsonSerde<AdjustBalance>()
    private val sinkSerde = jsonSerde<BalanceAdjusted>()

    @BeforeAll
    fun init() {
        val props = Properties().apply {
            put(APPLICATION_ID_CONFIG, "balance-adjustments-test")
            put(BOOTSTRAP_SERVERS_CONFIG, "mock/:9092")
        }
        topology = TopologyTestDriver(buildTopology(StreamsBuilder()), props)
        factory = ConsumerRecordFactory(StringSerializer(), sourceSerde.serializer())
    }

    @AfterAll
    fun teardown() = topology.close()

    @Test
    fun `Balance is updated`() {
        val accountId1 = "account_1"

        sendAdjustBalance(AdjustBalance(accountId1, +10))
        assertReceived(BalanceAdjusted(accountId1, +10, +10))

        sendAdjustBalance(AdjustBalance(accountId1, +30))
        assertReceived(BalanceAdjusted(accountId1, +40, +30))

        sendAdjustBalance(AdjustBalance(accountId1, -50))
        assertReceived(BalanceAdjusted(accountId1, -10, -50))
    }

    private fun assertReceived(ba: BalanceAdjusted) {
        with(readOutput()!!) {
            assertEquals(key(), ba.accountId)
            assertEquals(value(), ba)
        }
    }

    private fun sendAdjustBalance(ba: AdjustBalance) =
        topology.pipeInput(factory.create(INPUT_TOPIC, ba.accountId, ba))

    private fun readOutput(): ProducerRecord<String, BalanceAdjusted>? =
        topology.readOutput(OUTPUT_TOPIC, StringDeserializer(), sinkSerde.deserializer())
}
