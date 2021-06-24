import RxSwift
import GRDB
import UIExtensions

open class GrdbStorage {
    public var dbPool: DatabasePool
    private var dbsInTransaction = [Int: Database]()

    public init(databaseFilePath: String) {
        dbPool = try! DatabasePool(path: databaseFilePath)

        try? migrator.migrate(dbPool)
    }

    open var migrator: DatabaseMigrator {
        var migrator = DatabaseMigrator()

        migrator.registerMigration("createBlockchainStates") { db in
            try db.create(table: BlockchainState.databaseTableName) { t in
                t.column(BlockchainState.Columns.primaryKey.name, .text).notNull()
                t.column(BlockchainState.Columns.initialRestored.name, .boolean)

                t.primaryKey([BlockchainState.Columns.primaryKey.name], onConflict: .replace)
            }
        }

        migrator.registerMigration("createPeerAddresses") { db in
            try db.create(table: PeerAddress.databaseTableName) { t in
                t.column(PeerAddress.Columns.ip.name, .text).notNull()
                t.column(PeerAddress.Columns.score.name, .integer).notNull()

                t.primaryKey([PeerAddress.Columns.ip.name], onConflict: .ignore)
            }
        }

        migrator.registerMigration("createBlockHashes") { db in
            try db.create(table: BlockHash.databaseTableName) { t in
                t.column(BlockHash.Columns.headerHash.name, .text).notNull()
                t.column(BlockHash.Columns.height.name, .integer).notNull()
                t.column(BlockHash.Columns.sequence.name, .integer).notNull()

                t.primaryKey([BlockHash.Columns.headerHash.name], onConflict: .replace)
            }
        }

        migrator.registerMigration("createSentTransactions") { db in
            try db.create(table: SentTransaction.databaseTableName) { t in
                t.column(SentTransaction.Columns.dataHash.name, .text).notNull()
                t.column(SentTransaction.Columns.lastSendTime.name, .double).notNull()
                t.column(SentTransaction.Columns.retriesCount.name, .integer).notNull()

                t.primaryKey([SentTransaction.Columns.dataHash.name], onConflict: .replace)
            }
        }

        migrator.registerMigration("createPublicKeys") { db in
            try db.create(table: PublicKey.databaseTableName) { t in
                t.column(PublicKey.Columns.path.name, .text).notNull()
                t.column(PublicKey.Columns.account.name, .integer).notNull()
                t.column(PublicKey.Columns.index.name, .integer).notNull()
                t.column(PublicKey.Columns.external.name, .boolean).notNull()
                t.column(PublicKey.Columns.raw.name, .blob).notNull()
                t.column(PublicKey.Columns.keyHash.name, .blob).notNull()
                t.column(PublicKey.Columns.scriptHashForP2WPKH.name, .blob).notNull()

                t.primaryKey([PublicKey.Columns.path.name], onConflict: .replace)
            }

            try db.create(index: "by\(PublicKey.Columns.raw.name)", on: PublicKey.databaseTableName, columns: [PublicKey.Columns.raw.name])
            try db.create(index: "by\(PublicKey.Columns.keyHash.name)", on: PublicKey.databaseTableName, columns: [PublicKey.Columns.keyHash.name])
            try db.create(index: "by\(PublicKey.Columns.scriptHashForP2WPKH.name)", on: PublicKey.databaseTableName, columns: [PublicKey.Columns.scriptHashForP2WPKH.name])
        }

        migrator.registerMigration("createBlocks") { db in
            try db.create(table: Block.databaseTableName) { t in
                t.column(Block.Columns.version.name, .integer).notNull()
                t.column(Block.Columns.previousBlockHash.name, .text).notNull()
                t.column(Block.Columns.merkleRoot.name, .blob).notNull()
                t.column(Block.Columns.timestamp.name, .integer).notNull()
                t.column(Block.Columns.bits.name, .integer).notNull()
                t.column(Block.Columns.nonce.name, .integer).notNull()
                t.column(Block.Columns.headerHash.name, .text).notNull()
                t.column(Block.Columns.height.name, .integer).notNull()
                t.column(Block.Columns.stale.name, .boolean)

                t.primaryKey([Block.Columns.headerHash.name], onConflict: .abort)
            }

            try db.create(index: "by\(Block.Columns.height.name)", on: Block.databaseTableName, columns: [Block.Columns.height.name])
        }

        migrator.registerMigration("createTransactions") { db in
            try db.create(table: Transaction.databaseTableName) { t in
                t.column(Transaction.Columns.uid.name, .text).notNull()
                t.column(Transaction.Columns.dataHash.name, .text).notNull()
                t.column(Transaction.Columns.version.name, .integer).notNull()
                t.column(Transaction.Columns.lockTime.name, .integer).notNull()
                t.column(Transaction.Columns.timestamp.name, .integer).notNull()
                t.column(Transaction.Columns.order.name, .integer).notNull()
                t.column(Transaction.Columns.blockHash.name, .text)
                t.column(Transaction.Columns.isMine.name, .boolean)
                t.column(Transaction.Columns.isOutgoing.name, .boolean)
                t.column(Transaction.Columns.status.name, .integer)
                t.column(Transaction.Columns.segWit.name, .boolean)

                t.primaryKey([Transaction.Columns.dataHash.name], onConflict: .replace)
                t.foreignKey([Transaction.Columns.blockHash.name], references: Block.databaseTableName, columns: [Block.Columns.headerHash.name], onDelete: .cascade, onUpdate: .cascade)
            }
        }

        migrator.registerMigration("createInputs") { db in
            try db.create(table: Input.databaseTableName) { t in
                t.column(Input.Columns.previousOutputTxHash.name, .text).notNull()
                t.column(Input.Columns.previousOutputIndex.name, .integer).notNull()
                t.column(Input.Columns.signatureScript.name, .blob).notNull()
                t.column(Input.Columns.sequence.name, .integer).notNull()
                t.column(Input.Columns.transactionHash.name, .text).notNull()
                t.column(Input.Columns.keyHash.name, .blob)
                t.column(Input.Columns.address.name, .text)
                t.column(Input.Columns.witnessData.name, .blob)

                t.primaryKey([Input.Columns.previousOutputTxHash.name, Input.Columns.previousOutputIndex.name], onConflict: .abort)
                t.foreignKey([Input.Columns.transactionHash.name], references: Transaction.databaseTableName, columns: [Transaction.Columns.dataHash.name], onDelete: .cascade, onUpdate: .cascade, deferred: true)
            }
        }

        migrator.registerMigration("createOutputs") { db in
            try db.create(table: Output.databaseTableName) { t in
                t.column(Output.Columns.value.name, .integer).notNull()
                t.column(Output.Columns.lockingScript.name, .blob).notNull()
                t.column(Output.Columns.index.name, .integer).notNull()
                t.column(Output.Columns.transactionHash.name, .text).notNull()
                t.column(Output.Columns.publicKeyPath.name, .text)
                t.column(Output.Columns.changeOutput.name, .boolean)
                t.column(Output.Columns.scriptType.name, .integer)
                t.column(Output.Columns.keyHash.name, .blob)
                t.column(Output.Columns.address.name, .text)

                t.primaryKey([Output.Columns.transactionHash.name, Output.Columns.index.name], onConflict: .abort)
                t.foreignKey([Output.Columns.transactionHash.name], references: Transaction.databaseTableName, columns: [Transaction.Columns.dataHash.name], onDelete: .cascade, onUpdate: .cascade, deferred: true)
                t.foreignKey([Output.Columns.publicKeyPath.name], references: PublicKey.databaseTableName, columns: [PublicKey.Columns.path.name], onDelete: .setNull, onUpdate: .setNull)
            }
        }

        migrator.registerMigration("addConnectionTimeToPeerAddresses") { db in
            try db.alter(table: PeerAddress.databaseTableName) { t in
                t.add(column: PeerAddress.Columns.connectionTime.name, .double)
            }
        }

        migrator.registerMigration("addHasTransactionsToBlocks") { db in
            try db.alter(table: Block.databaseTableName) { t in
                t.add(column: Block.Columns.hasTransactions.name, .boolean).notNull().defaults(to: false)
            }

            try db.execute(sql: "UPDATE \(Block.databaseTableName) SET \(Block.Columns.hasTransactions.name) = true")
        }

        migrator.registerMigration("setCorrectTimestampForCheckpointBlock578592") { db in
            try db.execute(sql: "UPDATE \(Block.databaseTableName) SET \(Block.Columns.timestamp.name) = 1559256184 WHERE \(Block.Columns.height.name) == 578592 AND \(Block.Columns.timestamp.name) == 1559277784")
        }

        migrator.registerMigration("addRedeemScriptToOutput") { db in
            try db.alter(table: Output.databaseTableName) { t in
                t.add(column: Output.Columns.redeemScript.name, .blob)
            }
        }

        migrator.registerMigration("addPluginInfoToOutput") { db in
            try db.alter(table: Output.databaseTableName) { t in
                t.add(column: Output.Columns.pluginId.name, .integer)
                t.add(column: Output.Columns.pluginData.name, .text)
            }
        }

        migrator.registerMigration("addSendSuccessToSentTransaction") { db in
            try db.alter(table: SentTransaction.databaseTableName) { t in
                t.add(column: SentTransaction.Columns.sendSuccess.name, .boolean)
            }
        }

        migrator.registerMigration("createInvalidTransactions") { db in
            try db.create(table: InvalidTransaction.databaseTableName) { t in
                t.column(Transaction.Columns.uid.name, .text).notNull()
                t.column(Transaction.Columns.dataHash.name, .text).notNull()
                t.column(Transaction.Columns.version.name, .integer).notNull()
                t.column(Transaction.Columns.lockTime.name, .integer).notNull()
                t.column(Transaction.Columns.timestamp.name, .integer).notNull()
                t.column(Transaction.Columns.order.name, .integer).notNull()
                t.column(Transaction.Columns.blockHash.name, .text)
                t.column(Transaction.Columns.isMine.name, .boolean)
                t.column(Transaction.Columns.isOutgoing.name, .boolean)
                t.column(Transaction.Columns.status.name, .integer)
                t.column(Transaction.Columns.segWit.name, .boolean)
                t.column(Transaction.Columns.transactionInfoJson.name, .blob)
            }
        }

        migrator.registerMigration("addConflictingTxHashAndTxInfoToTransaction") { db in
            try db.alter(table: Transaction.databaseTableName) { t in
                t.add(column: Transaction.Columns.transactionInfoJson.name, .blob).defaults(to: Data())
                t.add(column: Transaction.Columns.conflictingTxHash.name, .text)
            }
        }

        migrator.registerMigration("addConflictingTxHashToInvalidTransaction") { db in
            try db.alter(table: InvalidTransaction.databaseTableName) { t in
                t.add(column: Transaction.Columns.conflictingTxHash.name, .text)
            }
        }

        migrator.registerMigration("addRawTransactionToTransactionAndInvalidTransaction") { db in
            try db.alter(table: Transaction.databaseTableName) { t in
                t.add(column: Transaction.Columns.rawTransaction.name, .text)
            }
            try db.alter(table: InvalidTransaction.databaseTableName) { t in
                t.add(column: Transaction.Columns.rawTransaction.name, .text)
            }
        }

        migrator.registerMigration("addFailedToSpendToOutputs") { db in
            try db.alter(table: Output.databaseTableName) { t in
                t.add(column: Output.Columns.failedToSpend.name, .boolean).notNull().defaults(to: false)
            }
        }

        return migrator
    }

    private func fullTransaction(transaction: Transaction) -> FullTransaction {
        FullTransaction(
                header: transaction,
                inputs: inputs(transactionHash: transaction.dataHash),
                outputs: outputs(transactionHash: transaction.dataHash)
        )
    }

    private func addWithoutTransaction(transaction: FullTransaction, db: Database) throws {
        try transaction.header.insert(db)

        for input in transaction.inputs {
            try input.insert(db)
        }

        for output in transaction.outputs {
            try output.insert(db)
        }
    }

    private func inputsWithPreviousOutputs(transactionHashes: [Data], db: Database) throws -> [InputWithPreviousOutput] {
        var inputs = [InputWithPreviousOutput]()

        let inputC = Input.Columns.allCases.count
        let outputC = Output.Columns.allCases.count

        let adapter = ScopeAdapter([
            "input": RangeRowAdapter(0..<inputC),
            "output": RangeRowAdapter(inputC..<inputC + outputC)
        ])

        let sql = """
                  SELECT inputs.*, outputs.*
                  FROM inputs
                  LEFT JOIN outputs ON inputs.previousOutputTxHash = outputs.transactionHash AND inputs.previousOutputIndex = outputs."index"
                  WHERE inputs.transactionHash IN (\(transactionHashes.map({ "x'" + $0.hex + "'" }).joined(separator: ",")))
                  """
        let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)

        while let row = try rows.next() {
            inputs.append(InputWithPreviousOutput(input: row["input"], previousOutput: row["output"]))
        }

        return inputs
    }

}

extension GrdbStorage: IStorage {
    // BlockchainState

    public var initialRestored: Bool? {
        try? dbPool.read { db in
            try BlockchainState.fetchOne(db)?.initialRestored
        }
    }

    public func set(initialRestored: Bool) {
        _ = try? dbPool.write { db in
            let state = try BlockchainState.fetchOne(db) ?? BlockchainState()
            state.initialRestored = initialRestored
            try state.insert(db)
        }
    }

    // PeerAddress

    public func leastScoreFastestPeerAddress(excludingIps: [String]) -> PeerAddress? {
        try? dbPool.read { db in
            try PeerAddress
                    .filter(!excludingIps.contains(PeerAddress.Columns.ip))
                    .order(PeerAddress.Columns.score.asc, PeerAddress.Columns.connectionTime.asc)
                    .fetchOne(db)
        }
    }

    public func peerAddressExist(address: String) -> Bool {
        var exist: Bool = false
        do {
            exist = try dbPool.read { db in
                try PeerAddress
                    .filter(PeerAddress.Columns.ip == address)
                    .fetchCount(db) > 0
            }
        } catch { }
        
        return exist
    }

    public func save(peerAddresses: [PeerAddress]) {
        _ = try? dbPool.write { db in
            for peerAddress in peerAddresses {
                try peerAddress.insert(db)
            }
        }
    }

    public func increasePeerAddressScore(ip: String) {
        _ = try? dbPool.write { db in
            if let peerAddress = try PeerAddress.filter(PeerAddress.Columns.ip == ip).fetchOne(db) {
                peerAddress.score += 1
                try peerAddress.save(db)
            }
        }
    }

    public func deletePeerAddress(byIp ip: String) {
        _ = try? dbPool.write { db in
            try PeerAddress.filter(PeerAddress.Columns.ip == ip).deleteAll(db)
        }
    }

    public func set(connectionTime: Double, toPeerAddress ip: String) {
        _ = try? dbPool.write { db in
            if let peerAddress = try PeerAddress.filter(PeerAddress.Columns.ip == ip).fetchOne(db) {
                peerAddress.connectionTime = connectionTime
                try peerAddress.save(db)
            }
        }
    }

    // BlockHash

    public var blockchainBlockHashes: [BlockHash] {
        var blockHashes: [BlockHash] = []
        do {
            blockHashes = try dbPool.read { db in
                try BlockHash.filter(BlockHash.Columns.height == 0).fetchAll(db)
            }
        } catch { }
        
        return blockHashes
    }

    public var lastBlockchainBlockHash: BlockHash? {
        try? dbPool.read { db in
            try BlockHash.filter(BlockHash.Columns.height == 0).order(BlockHash.Columns.sequence.desc).fetchOne(db)
        }
    }

    public var lastBlockHash: BlockHash? {
        try? dbPool.read { db in
            try BlockHash.order(BlockHash.Columns.sequence.desc).fetchOne(db)
        }
    }

    public var blockHashHeaderHashes: [Data] {
        var data: [Data] = []
        do {
            data = try dbPool.read { db in
                let rows = try Row.fetchCursor(db, sql: "SELECT headerHash from blockHashes")
                var hashes = [Data]()
                
                while let row = try rows.next() {
                    hashes.append(row[0] as Data)
                }
                
                return hashes
            }
        } catch { }
        
        return data
    }

    public func blockHashHeaderHashes(except excludedHashes: [Data]) -> [Data] {
        var data: [Data] = []
        do {
            data = try dbPool.read { db in
                let hashesExpression = excludedHashes.map { _ in "?" }.joined(separator: ",")
                let hashesArgs = StatementArguments(excludedHashes)
                let rows = try Row.fetchCursor(db, sql: "SELECT headerHash from blockHashes WHERE headerHash NOT IN (\(hashesExpression))", arguments: hashesArgs)
                var hexes = [Data]()
                
                while let row = try rows.next() {
                    hexes.append(row[0] as Data)
                }
                
                return hexes
            }
        } catch { }
        
        return data
    }

    public func blockHashesSortedBySequenceAndHeight(limit: Int) -> [BlockHash] {
        var blockHashes: [BlockHash] = []
        do {
            blockHashes = try dbPool.read { db in
                try BlockHash.order(BlockHash.Columns.sequence.asc, BlockHash.Columns.height.asc).limit(limit).fetchAll(db)
            }
        } catch { }
        
        return blockHashes
    }

    public func add(blockHashes: [BlockHash]) {
        _ = try? dbPool.write { db in
            for blockHash in blockHashes {
                try blockHash.insert(db)
            }
        }
    }

    public func deleteBlockHash(byHash hash: Data) {
        _ = try? dbPool.write { db in
            try BlockHash.filter(BlockHash.Columns.headerHash == hash).deleteAll(db)
        }
    }

    public func deleteBlockchainBlockHashes() {
        _ = try? dbPool.write { db in
            try BlockHash.filter(BlockHash.Columns.height == 0).deleteAll(db)
        }
    }

    public func deleteUselessBlocks(before height: Int) {
        _ = try? dbPool.write { db in
            try Block.filter(Block.Columns.height < height).filter(Block.Columns.hasTransactions == false).deleteAll(db)
        }
    }

    public func releaseMemory() {
        dbPool.releaseMemory()
    }

    // Block

    public var blocksCount: Int {
        var count: Int = 0
        do {
            count = try dbPool.read { db in
                try Block.fetchCount(db)
            }
        } catch { }
        
        return count
    }

    public var lastBlock: Block? {
        try? dbPool.read { db in
            try Block.order(Block.Columns.height.desc).fetchOne(db)
        }
    }

    public func blocksCount(headerHashes: [Data]) -> Int {
        var count: Int = 0
        do {
            count = try dbPool.read { db in
                try Block.filter(headerHashes.contains(Block.Columns.headerHash)).fetchCount(db)
            }
        } catch { }
        
        return count
    }

    public func update(block: Block) {
        _ = try? dbPool.write { db in
            try block.update(db)
        }
    }

    public func save(block: Block) {
        _ = try? dbPool.write { db in
            try block.insert(db)
        }
    }

    public func blocks(heightGreaterThan leastHeight: Int, sortedBy sortField: Block.Columns, limit: Int) -> [Block] {
        var blocks: [Block] = []
        do {
            blocks = try dbPool.read { db in
                try Block.filter(Block.Columns.height > leastHeight).order(sortField.desc).limit(limit).fetchAll(db)
            }
        } catch { }
        
        return blocks
    }

    public func blocks(from startHeight: Int, to endHeight: Int, ascending: Bool) -> [Block] {
        var blocks: [Block] = []
        do {
            blocks = try dbPool.read { db in
                try Block.filter(Block.Columns.height >= startHeight).filter(Block.Columns.height <= endHeight).order(ascending ? Block.Columns.height.asc : Block.Columns.height.desc).fetchAll(db)
            }
        } catch { }
        
        return blocks
    }

    public func blocks(byHexes hexes: [Data]) -> [Block] {
        var blocks: [Block] = []
        do {
            blocks = try dbPool.read { db in
                try Block.filter(hexes.contains(Block.Columns.headerHash)).fetchAll(db)
            }
        } catch { }
        
        return blocks
    }

    public func blocks(heightGreaterThanOrEqualTo height: Int, stale: Bool) -> [Block] {
        var blocks: [Block] = []
        do {
            blocks = try dbPool.read { db in
                try Block.filter(Block.Columns.stale == stale).filter(Block.Columns.height >= height).fetchAll(db)
            }
        } catch { }
        
        return blocks
    }

    public func blocks(stale: Bool) -> [Block] {
        var blocks: [Block] = []
        do {
            blocks = try dbPool.read { db in
                try Block.filter(Block.Columns.stale == stale).fetchAll(db)
            }
        } catch { }
        
        return blocks
    }

    public func blockByHeightStalePrioritized(height: Int) -> Block? {
        try? dbPool.read { db in
            try Block.filter(Block.Columns.height == height).order(Block.Columns.stale.desc).fetchOne(db)
        }
    }

    public func block(byHeight height: Int) -> Block? {
        try? dbPool.read { db in
            try Block.filter(Block.Columns.height == height).fetchOne(db)
        }
    }

    public func block(byHash hash: Data) -> Block? {
        try? dbPool.read { db in
            try Block.filter(Block.Columns.headerHash == hash).fetchOne(db)
        }
    }

    public func block(stale: Bool, sortedHeight: String) -> Block? {
        try? dbPool.read { db in
            let order = sortedHeight == "ASC" ? Block.Columns.height.asc : Block.Columns.height.desc
            return try Block.filter(Block.Columns.stale == stale).order(order).fetchOne(db)
        }
    }

    public func add(block: Block) throws {
        _ = try dbPool.write { db in
            try block.insert(db)
        }
    }

    public func delete(blocks: [Block]) throws {
        _ = try dbPool.write { db in
            for block in blocks {
                for transaction in transactions(ofBlock: block) {
                    try Input.filter(Input.Columns.transactionHash == transaction.dataHash).deleteAll(db)
                    try Output.filter(Output.Columns.transactionHash == transaction.dataHash).deleteAll(db)
                }

                try Transaction.filter(Transaction.Columns.blockHash == block.headerHash).deleteAll(db)
            }

            try Block.filter(blocks.map{$0.headerHash}.contains(Block.Columns.headerHash)).deleteAll(db)
        }
    }

    public func unstaleAllBlocks() throws {
        _ = try dbPool.write { db in
            try db.execute(sql: "UPDATE \(Block.databaseTableName) SET stale = ? WHERE stale = ?", arguments: [false, true])
        }
    }

    public func timestamps(from startHeight: Int, to endHeight: Int) -> [Int] {
        var timestamps: [Int] = []
        do {
            timestamps = try dbPool.read { db in
                var timestamps = [Int]()
                
                let sql = "SELECT blocks.timestamp FROM blocks WHERE blocks.height >= \(startHeight) AND blocks.height <= \(endHeight) ORDER BY blocks.timestamp ASC"
                let rows = try Row.fetchCursor(db, sql: sql)
                
                while let row = try rows.next() {
                    if let timestamp = Int.fromDatabaseValue(row["timestamp"]) {
                        timestamps.append(timestamp)
                    }
                }
                
                return timestamps
            }
        } catch { }
        
        return timestamps
    }

    // Transaction
    public func fullTransaction(byHash hash: Data) -> FullTransaction? {
        try? dbPool.read { db in
            try Transaction.filter(Transaction.Columns.dataHash == hash).fetchOne(db)
        }.flatMap { fullTransaction(transaction: $0) }
    }

    public func transaction(byHash hash: Data) -> Transaction? {
        try? dbPool.read { db in
            try Transaction.filter(Transaction.Columns.dataHash == hash).fetchOne(db)
        }
    }

    public func invalidTransaction(byHash hash: Data) -> InvalidTransaction? {
        try? dbPool.read { db in
            try InvalidTransaction.filter(Transaction.Columns.dataHash == hash).fetchOne(db)
        }
    }

    public func validOrInvalidTransaction(byUid uid: String) -> Transaction? {
        try? dbPool.read { db in
            let transactionC = Transaction.Columns.allCases.count
            
            let adapter = ScopeAdapter([
                "transaction": RangeRowAdapter(0..<transactionC)
            ])
            
            let sql = """
                      SELECT transactions.*
                      FROM (SELECT * FROM invalid_transactions UNION ALL SELECT transactions.* FROM transactions) AS transactions
                      WHERE transactions.uid = ?
                      """
            
            let rows = try Row.fetchCursor(db, sql: sql, arguments: [uid], adapter: adapter)
            
            if let row = try rows.next() {
                return row["transaction"]
            }
            
            return nil
        }
    }

    public func incomingPendingTransactionHashes() -> [Data] {
        var data: [Data] = []
        do {
            data = try dbPool.read { db in
                try Transaction
                    .filter(Transaction.Columns.blockHash == nil)
                    .filter(Transaction.Columns.isOutgoing == false)
                    .fetchAll(db)
            }.map { $0.dataHash }
        } catch { }
        
        return data
    }

    public func incomingPendingTransactionsExist() -> Bool {
        var exist: Bool = false
        do {
            exist = try dbPool.read { db in
                try Transaction
                    .filter(Transaction.Columns.blockHash == nil)
                    .filter(Transaction.Columns.isMine == true)
                    .filter(Transaction.Columns.isOutgoing == false)
                    .fetchCount(db) > 0
            }
        } catch { }
        
        return exist
    }

    public func inputs(byHashes hashes: [Data]) -> [Input] {
        var inputs: [Input] = []
        do {
            inputs = try dbPool.read { db in
                try Input.filter(hashes.contains(Input.Columns.transactionHash)).fetchAll(db)
            }
        } catch { }
        
        return inputs
    }

    public func transactionExists(byHash hash: Data) -> Bool {
        transaction(byHash: hash) != nil
    }

    public func transactions(ofBlock block: Block) -> [Transaction] {
        var transactions: [Transaction] = []
        do {
            transactions = try dbPool.read { db in
                try Transaction.filter(Transaction.Columns.blockHash == block.headerHash).fetchAll(db)
            }
        } catch { }
        
        return transactions
    }

    public func newTransactions() -> [FullTransaction] {
        var transactions: [FullTransaction] = []
        do {
            transactions = try dbPool.read { db in
                try Transaction.filter(Transaction.Columns.status == TransactionStatus.new).fetchAll(db)
            }.map { fullTransaction(transaction: $0) }
        } catch { }
        
        return transactions
    }

    public func newTransaction(byHash hash: Data) -> Transaction? {
        try? dbPool.read { db in
            try Transaction
                .filter(Transaction.Columns.status == TransactionStatus.new)
                .filter(Transaction.Columns.dataHash == hash)
                .fetchOne(db)
        }
    }

    public func relayedTransactionExists(byHash hash: Data) -> Bool {
        var exist: Bool = false
        do {
            exist = try dbPool.read { db in
                try Transaction
                    .filter(Transaction.Columns.status == TransactionStatus.relayed)
                    .filter(Transaction.Columns.dataHash == hash)
                    .fetchCount(db) > 1
            }
        } catch { }
        
        return exist
    }

    public func add(transaction: FullTransaction) throws {
        _ = try dbPool.write { db in
            try addWithoutTransaction(transaction: transaction, db: db)
        }
    }

    public func update(transaction: FullTransaction) throws {
        _ = try dbPool.write { db in
            try transaction.header.update(db)
            for input in transaction.inputs {
                try input.update(db)
            }
            for output in transaction.outputs {
                try output.update(db)
            }
        }
    }

    public func update(transaction: Transaction) throws {
        _ = try dbPool.write { db in
            try transaction.update(db)
        }
    }

    public func fullInfo(forTransactions transactionsWithBlocks: [TransactionWithBlock]) -> [FullTransactionForInfo] {
        let transactionHashes: [Data] = transactionsWithBlocks.filter({ $0.transaction.status != .invalid }).map({ $0.transaction.dataHash })
        var inputs = [InputWithPreviousOutput]()
        var outputs = [Output]()

        try? dbPool.read { db in
            for transactionHashChunks in transactionHashes.chunked(into: 999) {
                inputs.append(contentsOf: try inputsWithPreviousOutputs(transactionHashes: transactionHashChunks, db: db))
                outputs.append(contentsOf: try Output.filter(transactionHashChunks.contains(Output.Columns.transactionHash)).fetchAll(db))
            }
        }

        let inputsByTransaction: [Data: [InputWithPreviousOutput]] = Dictionary(grouping: inputs, by: { $0.input.transactionHash })
        let outputsByTransaction: [Data: [Output]] = Dictionary(grouping: outputs, by: { $0.transactionHash })
        var results = [FullTransactionForInfo]()

        for transactionWithBlock in transactionsWithBlocks {
            let fullTransaction = FullTransactionForInfo(
                    transactionWithBlock: transactionWithBlock,
                    inputsWithPreviousOutputs: inputsByTransaction[transactionWithBlock.transaction.dataHash] ?? [],
                    outputs: outputsByTransaction[transactionWithBlock.transaction.dataHash] ?? []
            )

            results.append(fullTransaction)
        }

        return results
    }

    public func transactionFullInfo(byHash hash: Data) -> FullTransactionForInfo? {
        var transaction: TransactionWithBlock? = nil

        try? dbPool.read { db in
            let transactionC = Transaction.Columns.allCases.count

            let adapter = ScopeAdapter([
                "transaction": RangeRowAdapter(0..<transactionC)
            ])

            let sql = """
                      SELECT transactions.*, blocks.height as blockHeight
                      FROM transactions
                      LEFT JOIN blocks ON transactions.blockHash = blocks.headerHash
                      WHERE transactions.dataHash = \("x'" + hash.hex + "'")                    
                      """

            let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)

            if let row = try rows.next() {
                transaction = TransactionWithBlock(transaction: row["transaction"], blockHeight: row["blockHeight"])
            }

        }

        guard let transactionWithBlock = transaction else {
            return nil
        }
        return fullInfo(forTransactions: [transactionWithBlock]).first
    }

    public func validOrInvalidTransactionsFullInfo(fromTimestamp: Int?, fromOrder: Int?, limit: Int?) -> [FullTransactionForInfo] {
        var transactions = [TransactionWithBlock]()

        try? dbPool.read { db in
            let transactionC = Transaction.Columns.allCases.count + 1

            let adapter = ScopeAdapter([
                "transaction": RangeRowAdapter(0..<transactionC)
            ])

            var sql = """
                      SELECT transactions.*, blocks.height as blockHeight
                      FROM (SELECT * FROM invalid_transactions UNION ALL SELECT transactions.* FROM transactions) AS transactions
                      LEFT JOIN blocks ON transactions.blockHash = blocks.headerHash
                      """

            if let fromTimestamp = fromTimestamp, let fromOrder = fromOrder {
                sql = sql + " WHERE transactions.timestamp < \(fromTimestamp) OR (transactions.timestamp == \(fromTimestamp) AND transactions.\"order\" < \(fromOrder))"
            }

            sql += " ORDER BY transactions.timestamp DESC, transactions.\"order\" DESC"

            if let limit = limit {
                sql += " LIMIT \(limit)"
            }

            let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)

            while let row = try rows.next() {
                let status: TransactionStatus = row[Transaction.Columns.status]
                let transaction: Transaction

                if status == .invalid {
                    let invalidTransaction: InvalidTransaction = row["transaction"]
                    transaction = invalidTransaction
                } else {
                    transaction = row["transaction"]
                }

                transactions.append(TransactionWithBlock(transaction: transaction, blockHeight: row["blockHeight"]))
            }

        }

        return fullInfo(forTransactions: transactions)
    }

    public func moveTransactionsTo(invalidTransactions: [InvalidTransaction]) throws {
        try dbPool.writeInTransaction { db in
            for invalidTransaction in invalidTransactions {
                try invalidTransaction.insert(db)

                let inputs = try inputsWithPreviousOutputs(transactionHashes: [invalidTransaction.dataHash], db: db)
                for input in inputs {
                    if let previousOutput = input.previousOutput {
                        previousOutput.failedToSpend = true
                        try previousOutput.update(db)
                    }
                }

                try Input.filter(Input.Columns.transactionHash == invalidTransaction.dataHash).deleteAll(db)
                try Output.filter(Output.Columns.transactionHash == invalidTransaction.dataHash).deleteAll(db)
                try Transaction.filter(Transaction.Columns.dataHash == invalidTransaction.dataHash).deleteAll(db)
            }

            return .commit
        }
    }

    public func move(invalidTransaction: InvalidTransaction, toTransactions transaction: FullTransaction) throws {
        try dbPool.writeInTransaction { db in
            try addWithoutTransaction(transaction: transaction, db: db)
            try InvalidTransaction.filter(Transaction.Columns.uid == invalidTransaction.uid).deleteAll(db)

            return .commit
        }
    }

    // Inputs and Outputs

    public func outputsWithPublicKeys() -> [OutputWithPublicKey] {
        var outputs: [OutputWithPublicKey] = []
        do {
            outputs = try dbPool.read { db in
                let outputC = Output.Columns.allCases.count
                let publicKeyC = PublicKey.Columns.allCases.count
                let inputC = Input.Columns.allCases.count
                
                let adapter = ScopeAdapter([
                    "output": RangeRowAdapter(0..<outputC),
                    "publicKey": RangeRowAdapter(outputC..<outputC + publicKeyC),
                    "input": RangeRowAdapter(outputC + publicKeyC..<outputC + publicKeyC + inputC)
                ])
                
                let sql = """
                      SELECT outputs.*, publicKeys.*, inputs.*, blocks.height AS blockHeight
                      FROM outputs
                      INNER JOIN publicKeys ON outputs.publicKeyPath = publicKeys.path
                      LEFT JOIN inputs ON inputs.previousOutputTxHash = outputs.transactionHash AND inputs.previousOutputIndex = outputs."index"
                      LEFT JOIN transactions ON inputs.transactionHash = transactions.dataHash
                      LEFT JOIN blocks ON transactions.blockHash = blocks.headerHash
                      """
                let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)
                
                var outputs = [OutputWithPublicKey]()
                while let row = try rows.next() {
                    outputs.append(OutputWithPublicKey(output: row["output"], publicKey: row["publicKey"], spendingInput: row["input"], spendingBlockHeight: row["blockHeight"]))
                }
                
                return outputs
            }
        } catch { }
        
        return outputs
    }

    public func unspentOutputs() -> [UnspentOutput] {
        var unspentOutputs: [UnspentOutput] = []
        do {
            unspentOutputs = try dbPool.read { db in
                let inputs = try Input.fetchAll(db)
                
                let outputC = Output.Columns.allCases.count
                let publicKeyC = PublicKey.Columns.allCases.count
                let transactionC = Transaction.Columns.allCases.count
                
                let adapter = ScopeAdapter([
                    "output": RangeRowAdapter(0..<outputC),
                    "publicKey": RangeRowAdapter(outputC..<outputC + publicKeyC),
                    "transaction": RangeRowAdapter(outputC + publicKeyC..<outputC + publicKeyC + transactionC)
                ])
                
                let sql = """
                      SELECT outputs.*, publicKeys.*, transactions.*, blocks.height AS blockHeight
                      FROM outputs
                      INNER JOIN publicKeys ON outputs.publicKeyPath = publicKeys.path
                      INNER JOIN transactions ON outputs.transactionHash = transactions.dataHash
                      LEFT JOIN blocks ON transactions.blockHash = blocks.headerHash
                      WHERE outputs.scriptType != \(ScriptType.unknown.rawValue)
                      """
                let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)
                
                var outputs = [UnspentOutput]()
                while let row = try rows.next() {
                    let output: Output = row["output"]
                    
                    if !inputs.contains(where: { $0.previousOutputTxHash == output.transactionHash && $0.previousOutputIndex == output.index }) {
                        outputs.append(UnspentOutput(output: output, publicKey: row["publicKey"], transaction: row["transaction"], blockHeight: row["blockHeight"]))
                    }
                }
                
                return outputs
            }
        } catch { }
        
        return unspentOutputs
    }

    public func inputs(transactionHash: Data) -> [Input] {
        var inputs: [Input] = []
        do {
            inputs = try dbPool.read { db in
                try Input.filter(Input.Columns.transactionHash == transactionHash).fetchAll(db)
            }
        } catch { }
     
        return inputs
    }

    public func outputs(transactionHash: Data) -> [Output] {
        var outputs: [Output] = []
        do {
            outputs = try dbPool.read { db in
                try Output.filter(Output.Columns.transactionHash == transactionHash).fetchAll(db)
            }
        } catch { }
        
        return outputs
    }

    public func previousOutput(ofInput input: Input) -> Output? {
        try? dbPool.read { db in
            try Output
                    .filter(Output.Columns.transactionHash == input.previousOutputTxHash)
                    .filter(Output.Columns.index == input.previousOutputIndex)
                    .fetchOne(db)
        }
    }

    public func inputsUsingOutputs(withTransactionHash transactionHash: Data) -> [Input] {
        var inputs: [Input] = []
        do {
            inputs = try dbPool.read { db in
                try Input.filter(Input.Columns.previousOutputTxHash == transactionHash).fetchAll(db)
            }
        } catch { }
        
        return inputs
    }

    public func inputsUsing(previousOutputTxHash: Data, previousOutputIndex: Int) -> [Input] {
        var inputs: [Input] = []
        do {
            inputs = try dbPool.read { db in
                try Input.filter(Input.Columns.previousOutputTxHash == previousOutputTxHash)
                    .filter(Input.Columns.previousOutputIndex == previousOutputIndex)
                    .fetchAll(db)
            }
        } catch { }
        
        return inputs
    }

    // SentTransaction
    public func sentTransaction(byHash hash: Data) -> SentTransaction? {
        try? dbPool.read { db in
            try SentTransaction.filter(SentTransaction.Columns.dataHash == hash).fetchOne(db)
        }
    }

    public func update(sentTransaction: SentTransaction) {
        _ = try? dbPool.write { db in
            try sentTransaction.update(db)
        }
    }

    public func delete(sentTransaction: SentTransaction) {
        _ = try? dbPool.write { db in
            try sentTransaction.delete(db)
        }
    }

    public func add(sentTransaction: SentTransaction) {
        _ = try? dbPool.write { db in
            try sentTransaction.insert(db)
        }
    }

    // PublicKeys
    public func publicKeys() -> [PublicKey] {
        var publicKeys: [PublicKey] = []
        do {
            publicKeys = try dbPool.read { db in
                try PublicKey.fetchAll(db)
            }
        } catch { }
        
        return publicKeys
    }

    public func publicKey(byScriptHashForP2WPKH hash: Data) -> PublicKey? {
        try? dbPool.read { db in
            try PublicKey.filter(PublicKey.Columns.scriptHashForP2WPKH == hash).fetchOne(db)
        }
    }

    public func publicKey(byRawOrKeyHash hash: Data) -> PublicKey? {
        try? dbPool.read { db in
            try PublicKey.filter(PublicKey.Columns.raw == hash || PublicKey.Columns.keyHash == hash).fetchOne(db)
        }
    }

    public func add(publicKeys: [PublicKey]) {
        _ = try? dbPool.write { db in
            for publicKey in publicKeys {
                try publicKey.insert(db)
            }
        }
    }

    public func publicKeysWithUsedState() -> [PublicKeyWithUsedState] {
        var publicKeys: [PublicKeyWithUsedState] = []
        do {
            publicKeys = try dbPool.read { db in
                let publicKeyC = PublicKey.Columns.allCases.count
                
                let adapter = ScopeAdapter([
                    "publicKey": RangeRowAdapter(0..<publicKeyC)
                ])
                
                let sql = """
                      SELECT publicKeys.*, outputs.transactionHash
                      FROM publicKeys
                      LEFT JOIN outputs ON publicKeys.path = outputs.publicKeyPath
                      GROUP BY publicKeys.path
                      """
                
                let rows = try Row.fetchCursor(db, sql: sql, adapter: adapter)
                var publicKeys = [PublicKeyWithUsedState]()
                while let row = try rows.next() {
                    publicKeys.append(PublicKeyWithUsedState(publicKey: row["publicKey"], used: row["transactionHash"] != nil))
                }
                
                return publicKeys
            }
        } catch { }
        
        return publicKeys
    }

    public func publicKey(byPath path: String) -> PublicKey? {
        try? dbPool.read { db in
            try PublicKey.filter(PublicKey.Columns.path == path).fetchOne(db)
        }
    }

}
