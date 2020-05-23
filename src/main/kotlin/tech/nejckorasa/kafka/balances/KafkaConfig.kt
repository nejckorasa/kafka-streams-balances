package tech.nejckorasa.kafka.balances

object KafkaConfig {
    val BOOTSTRAP_SERVERS get() = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?:  "localhost:9092"

    const val INPUT_TOPIC = "adjust-balance"
    const val OUTPUT_TOPIC = "balance-adjusted"
    const val BALANCES_STORE = "balance-store"

    fun topics() = listOf(INPUT_TOPIC, OUTPUT_TOPIC)
}