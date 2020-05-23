package tech.nejckorasa.kafka.balances.model

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import tech.nejckorasa.kafka.balances.model.JsonSerde.jsonSerde
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

object JsonSerde {
    inline fun <reified T> jsonSerde(): Serde<T> = object : Serde<T> {
        private val mapper = jacksonObjectMapper()
        override fun deserializer(): Deserializer<T> = Deserializer { _, data -> mapper.readValue(data, T::class.java) }
        override fun serializer(): Serializer<T> = Serializer { _, data -> mapper.writeValueAsBytes(data) }
    }
}

class AdjustBalanceSerializer : Serializer<AdjustBalance> {
    private val ser = jsonSerde<AdjustBalance>().serializer()
    override fun serialize(topic: String?, data: AdjustBalance?): ByteArray = ser.serialize(topic, data)
}