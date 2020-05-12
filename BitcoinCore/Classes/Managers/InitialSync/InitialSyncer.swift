import HdWalletKit
import RxSwift
import HsToolKit

class InitialSyncer {
    weak var delegate: IInitialSyncerDelegate?

    private var disposeBag = DisposeBag()

    private let storage: IStorage
    private let blockDiscovery: IBlockDiscovery
    private let publicKeyManager: IPublicKeyManager

    private let logger: Logger?
    private let async: Bool
    private let errorStorage: ErrorStorage?

    init(storage: IStorage, blockDiscovery: IBlockDiscovery, publicKeyManager: IPublicKeyManager,
         async: Bool = true, logger: Logger? = nil, errorStorage: ErrorStorage? = nil) {
        self.storage = storage
        self.blockDiscovery = blockDiscovery
        self.publicKeyManager = publicKeyManager

        self.logger = logger
        self.async = async
        self.errorStorage = errorStorage
    }

    private func sync(forAccount account: Int) {
        let externalObservable = blockDiscovery.discoverBlockHashes(account: account, external: true).asObservable()
        let internalObservable = blockDiscovery.discoverBlockHashes(account: account, external: false).asObservable()

        var observable = Observable
                .concat(externalObservable, internalObservable)
                .toArray()
                .map { array -> ([PublicKey], [BlockHash]) in
                    let (externalKeys, externalBlockHashes) = array[0]
                    let (internalKeys, internalBlockHashes) = array[1]
                    let sortedUniqueBlockHashes = Array<BlockHash>(externalBlockHashes + internalBlockHashes).unique.sorted { a, b in a.height < b.height }

                    return (externalKeys + internalKeys, sortedUniqueBlockHashes)
                }

        if async {
            observable = observable.subscribeOn(ConcurrentDispatchQueueScheduler(qos: .background))
        }

        observable.subscribe(onSuccess: { [weak self] keys, responses in
                    self?.handle(forAccount: account, keys: keys, blockHashes: responses)
                }, onError: { [weak self] error in
                    self?.handle(error: error)
                })
                .disposed(by: disposeBag)
    }

    private func handle(forAccount account: Int, keys: [PublicKey], blockHashes: [BlockHash]) {
        logger?.debug("Account \(account) has \(keys.count) keys and \(blockHashes.count) blocks")
        publicKeyManager.addKeys(keys: keys)

        // If gap shift is found
        if blockHashes.isEmpty {
            handleSuccess()
        } else {
            storage.add(blockHashes: blockHashes)
            sync(forAccount: account + 1)
        }
    }

    private func handleSuccess() {
        delegate?.onSyncSuccess()
    }

    private func handle(error: Error) {
        logger?.error(error)
        errorStorage?.add(apiError: error)
        delegate?.onSyncFailed(error: error)
    }

}

extension InitialSyncer: IInitialSyncer {

    func sync() {
        sync(forAccount: 0)
    }

    func terminate() {
        disposeBag = DisposeBag()
    }

}
