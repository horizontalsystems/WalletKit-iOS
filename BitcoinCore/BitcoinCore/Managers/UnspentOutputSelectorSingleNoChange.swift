import Foundation

public class UnspentOutputSelectorSingleNoChange {

    private let calculator: ITransactionSizeCalculator
    private let provider: IUnspentOutputProvider

    public init(calculator: ITransactionSizeCalculator, provider: IUnspentOutputProvider) {
        self.calculator = calculator
        self.provider = provider
    }

}

extension UnspentOutputSelectorSingleNoChange: IUnspentOutputSelector {

    public func select(value: Int, feeRate: Int, outputScriptType: ScriptType = .p2pkh, changeType: ScriptType = .p2pkh, senderPay: Bool, pluginDataOutputSize: Int) throws -> SelectedUnspentOutputInfo {
        let unspentOutputs = provider.spendableUtxo

        guard value > 0 else {
            throw BitcoinCoreErrors.SendValueErrors.wrongValue
        }
        guard !unspentOutputs.isEmpty else {
            throw BitcoinCoreErrors.SendValueErrors.emptyOutputs
        }
        let dust = (calculator.inputSize(type: changeType) + calculator.outputSize(type: changeType)) * feeRate // fee needed for make changeOutput, we use only p2pkh for change output

        // try to find 1 unspent output with exactly matching value
        for unspentOutput in unspentOutputs {
            let output = unspentOutput.output
            let fee = calculator.transactionSize(inputs: [output.scriptType], outputScriptTypes: [outputScriptType], pluginDataOutputSize: pluginDataOutputSize) * feeRate
            let totalFee = senderPay ? fee : 0
            if (value + totalFee <= output.value) && (value + totalFee + dust > output.value) {
                return SelectedUnspentOutputInfo(unspentOutputs: [unspentOutput], totalValue: output.value, fee: senderPay ? (output.value - value) : fee, addChangeOutput: false)
            }
        }

        throw BitcoinCoreErrors.SendValueErrors.notEnough(maxFee: 0)
    }

}
