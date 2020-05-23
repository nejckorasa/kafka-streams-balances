package tech.nejckorasa.kafka.balances

import tech.nejckorasa.kafka.balances.model.JsonSerde.jsonSerde
import tech.nejckorasa.kafka.balances.KafkaConfig.BOOTSTRAP_SERVERS
import tech.nejckorasa.kafka.balances.KafkaConfig.BALANCES_STORE
import tech.nejckorasa.kafka.balances.KafkaConfig.INPUT_TOPIC
import tech.nejckorasa.kafka.balances.KafkaConfig.OUTPUT_TOPIC

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.streams.*
import org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.kstream.TransformerSupplier
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.processor.WallclockTimestampExtractor
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores.keyValueStoreBuilder
import org.apache.kafka.streams.state.Stores.persistentKeyValueStore
import tech.nejckorasa.kafka.balances.model.AccountId
import tech.nejckorasa.kafka.balances.model.AdjustBalance
import tech.nejckorasa.kafka.balances.model.Amount
import tech.nejckorasa.kafka.balances.model.BalanceAdjusted
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
     */
    fun buildTopology(sb: StreamsBuilder): Topology {
        val sourceSerde = Consumed.with(
            Serdes.String(),
            jsonSerde<AdjustBalance>(),
            WallclockTimestampExtractor(),
            EARLIEST
        )
        val sinkSerde = Produced.with(Serdes.String(), jsonSerde<BalanceAdjusted>())

        sb.addStateStore(keyValueStoreBuilder(persistentKeyValueStore(BALANCES_STORE), StringSerde(), LongSerde()))
        sb.stream(INPUT_TOPIC, sourceSerde)
            .transform(adjustBalanceTransformer(), BALANCES_STORE)
            .to(OUTPUT_TOPIC, sinkSerde)

        return sb.build()
    }

    private fun adjustBalanceTransformer(): TransformerSupplier<AccountId, AdjustBalance, KeyValue<AccountId, BalanceAdjusted>> =
        TransformerSupplier {
            object : Transformer<AccountId, AdjustBalance, KeyValue<AccountId, BalanceAdjusted>> {
                private lateinit var context: ProcessorContext
                private lateinit var store: KeyValueStore<AccountId, Amount>

                override fun init(context: ProcessorContext) {
                    this.context = context
                    this.store = context.getStateStore(BALANCES_STORE) as KeyValueStore<AccountId, Amount>
                }

                override fun transform(key: AccountId, value: AdjustBalance): KeyValue<AccountId, BalanceAdjusted>? {
                    return when (val existingBalance: Amount? = store.get(key)) {
                        null -> {
                            store.put(key, value.adjustedAmount)
                            KeyValue.pair(key, BalanceAdjusted(key, value.adjustedAmount, value.adjustedAmount))
                        }
                        else -> {
                            val newBalance = existingBalance + value.adjustedAmount
                            store.put(key, newBalance)
                            KeyValue.pair(key, BalanceAdjusted(key, newBalance, value.adjustedAmount))
                        }
                    }
                }

                override fun close() {}
            }
        }
}