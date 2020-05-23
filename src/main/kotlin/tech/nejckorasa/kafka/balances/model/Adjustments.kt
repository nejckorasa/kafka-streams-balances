package tech.nejckorasa.kafka.balances.model


typealias Amount = Long
typealias AccountId = String

// input
data class AdjustBalance(val accountId: AccountId, val adjustedAmount: Amount)

// output
sealed class BalanceAdjustment {
    companion object {
        fun accepted(ab: AdjustBalance, newBalance: Amount): BalanceAdjustmentAccepted {
            return BalanceAdjustmentAccepted(ab.accountId, newBalance, ab.adjustedAmount)
        }

        fun rejected(ab: AdjustBalance, reason: String = "Insufficient funds"): BalanceAdjustmentRejected {
            return BalanceAdjustmentRejected(ab.accountId, ab.adjustedAmount, reason)
        }
    }
}

data class BalanceAdjustmentAccepted(val accountId: AccountId, val balance: Amount, val adjustedAmount: Amount) : BalanceAdjustment()
data class BalanceAdjustmentRejected(val accountId: AccountId, val adjustedAmount: Amount, val reason: String) : BalanceAdjustment()