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
import tech.nejckorasa.kafka.balances.KafkaConfig.REJECTED_TOPIC
import tech.nejckorasa.kafka.balances.model.AdjustBalance
import tech.nejckorasa.kafka.balances.model.BalanceAdjustmentAccepted
import tech.nejckorasa.kafka.balances.model.BalanceAdjustmentRejected
import java.util.*

@TestInstance(PER_CLASS)
class BalancesStreamTest {

    private lateinit var factory: ConsumerRecordFactory<String, AdjustBalance>
    private lateinit var topology: TopologyTestDriver

    private val sourceSerde = jsonSerde<AdjustBalance>()
    private val sinkSerde = jsonSerde<BalanceAdjustmentAccepted>()
    private val rejectedSinkSerde = jsonSerde<BalanceAdjustmentRejected>()

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
    fun `Balances are updated`() {
        val accountId1 = "account_1"

        sendAdjustBalance(AdjustBalance(accountId1, adjustedAmount = +10))
        assertReceived(BalanceAdjustmentAccepted(accountId1, balance = +10, adjustedAmount = +10))

        sendAdjustBalance(AdjustBalance(accountId1, adjustedAmount = +30))
        assertReceived(BalanceAdjustmentAccepted(accountId1, balance = +40, adjustedAmount = +30))
    }

    @Test
    fun `Overdrafts are rejected`() {
        val accountId1 = "account_2"

        sendAdjustBalance(AdjustBalance(accountId1, adjustedAmount = +40))
        assertReceived(BalanceAdjustmentAccepted(accountId1, balance = +40, adjustedAmount = +40))

        sendAdjustBalance(AdjustBalance(accountId1, adjustedAmount = -50))
        assertReceivedRejected(BalanceAdjustmentRejected(accountId1, adjustedAmount = -50, reason = "Insufficient funds"))

        sendAdjustBalance(AdjustBalance(accountId1, adjustedAmount = -40))
        assertReceived(BalanceAdjustmentAccepted(accountId1, balance = +0, adjustedAmount = -40))

    }

    private fun assertReceived(ba: BalanceAdjustmentAccepted) {
        with(readOutput()!!) {
            assertEquals(ba.accountId, key())
            assertEquals(ba, value())
        }
    }

    private fun assertReceivedRejected(ba: BalanceAdjustmentRejected) {
        with(readRejectedOutput()!!) {
            assertEquals(ba.accountId, key())
            assertEquals(ba, value())
        }
    }

    private fun sendAdjustBalance(ba: AdjustBalance) =
        topology.pipeInput(factory.create(INPUT_TOPIC, ba.accountId, ba))

    private fun readOutput(): ProducerRecord<String, BalanceAdjustmentAccepted>? =
        topology.readOutput(OUTPUT_TOPIC, StringDeserializer(), sinkSerde.deserializer())

    private fun readRejectedOutput(): ProducerRecord<String, BalanceAdjustmentRejected>? =
        topology.readOutput(REJECTED_TOPIC, StringDeserializer(), rejectedSinkSerde.deserializer())
}
