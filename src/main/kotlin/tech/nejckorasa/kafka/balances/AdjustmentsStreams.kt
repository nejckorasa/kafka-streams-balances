package tech.nejckorasa.kafka.balances

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.*
import org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST
import org.apache.kafka.streams.kstream.*
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores.keyValueStoreBuilder
import org.apache.kafka.streams.state.Stores.persistentKeyValueStore
import tech.nejckorasa.kafka.balances.KafkaConfig.BALANCES_STORE
import tech.nejckorasa.kafka.balances.KafkaConfig.BOOTSTRAP_SERVERS
import tech.nejckorasa.kafka.balances.KafkaConfig.INPUT_TOPIC
import tech.nejckorasa.kafka.balances.KafkaConfig.OUTPUT_TOPIC
import tech.nejckorasa.kafka.balances.KafkaConfig.REJECTED_TOPIC
import tech.nejckorasa.kafka.balances.model.*
import tech.nejckorasa.kafka.balances.model.JsonSerde.jsonSerde
import java.util.*

@Suppress("UNCHECKED_CAST")
object AdjustmentsStreams {

    operator fun invoke(): KafkaStreams = KafkaStreams(
        buildTopology(StreamsBuilder()),
        Properties().apply {
            put(StreamsConfig.APPLICATION_ID_CONFIG, "balance-adjustments")
            put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        })

    /**
     * Builds topology specifying the computational logic to process AdjustBalance records
     *
     * AdjustBalance records that retain positive balance are transformed and sent to `OUTPUT_TOPIC`
     * AdjustBalance records that would result in an overdraft are rejected and sent to `REJECTED_TOPIC`
     */
    fun buildTopology(sb: StreamsBuilder): Topology {
        val sourceSerde = Consumed.with(
            Serdes.String(),
            jsonSerde<AdjustBalance>(),
            WallclockTimestampExtractor(),
            EARLIEST
        )
        val successSinkSerde = Produced.with(Serdes.String(), jsonSerde<BalanceAdjustmentAccepted>())
        val rejectedSinkSerde = Produced.with(Serdes.String(), jsonSerde<BalanceAdjustmentRejected>())

        sb.addStateStore(keyValueStoreBuilder(persistentKeyValueStore(BALANCES_STORE), StringSerde(), LongSerde()))
        sb.stream(INPUT_TOPIC, sourceSerde)
            .transform(adjustBalanceTransformer(), BALANCES_STORE)
            .branch(Predicate { _, v -> v is BalanceAdjustmentAccepted },
                Predicate { _, v -> v is BalanceAdjustmentRejected }
            ).apply {
                get(0).mapValues { _, v -> v as BalanceAdjustmentAccepted }.to(OUTPUT_TOPIC, successSinkSerde)
                get(1).mapValues { _, v -> v as BalanceAdjustmentRejected }.to(REJECTED_TOPIC, rejectedSinkSerde)
            }

        return sb.build()
    }

    private fun adjustBalanceTransformer(): TransformerSupplier<AccountId, AdjustBalance, KeyValue<AccountId, BalanceAdjustment>> =
        TransformerSupplier {
            object : Transformer<AccountId, AdjustBalance, KeyValue<AccountId, BalanceAdjustment>> {
                private lateinit var context: ProcessorContext
                private lateinit var store: KeyValueStore<AccountId, Amount>

                override fun init(context: ProcessorContext) {
                    this.context = context
                    this.store = context.getStateStore(BALANCES_STORE) as KeyValueStore<AccountId, Amount>
                }

                override fun transform(key: AccountId, value: AdjustBalance): KeyValue<AccountId, BalanceAdjustment>? {
                    val existingBalance: Amount? = store.get(key)
                    val newBalance = (existingBalance ?: 0) + value.adjustedAmount

                    if (newBalance < 0) return KeyValue.pair(key, BalanceAdjustment.rejected(value))

                    store.put(key, newBalance)
                    return KeyValue.pair(key, BalanceAdjustment.accepted(value, newBalance))
                }

                override fun close() {}
            }
        }
}