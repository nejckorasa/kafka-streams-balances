package tech.nejckorasa.kafka.balances.model


typealias Amount = Long
typealias AccountId = String

data class BalanceAdjusted(val accountId: AccountId, val balance: Amount, val adjustedAmount: Amount)
data class AdjustBalance(val accountId: AccountId, val adjustedAmount: Amount)