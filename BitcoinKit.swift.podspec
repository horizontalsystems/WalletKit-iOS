Pod::Spec.new do |s|
  s.name             = 'BitcoinKit.swift'
  s.module_name      = 'BitcoinKit'
  s.version          = '0.11.0'
  s.summary          = 'Bitcoin library for Swift.'

  s.description      = <<-DESC
BitcoinKit implements Bitcoin protocol in Swift.
                       DESC

  s.homepage         = 'https://github.com/horizontalsystems/bitcoin-kit-ios'
  s.license          = { :type => 'MIT', :file => 'LICENSE' }
  s.author           = { 'Horizontal Systems' => 'hsdao@protonmail.ch' }
  s.source           = { git: 'https://github.com/horizontalsystems/bitcoin-kit-ios.git', tag: "#{s.version}" }
  s.social_media_url = 'http://horizontalsystems.io/'

  s.ios.deployment_target = '11.0'
  s.swift_version = '5'

  s.source_files = 'BitcoinKit/Classes/**/*'

  s.requires_arc = true

  s.dependency 'BitcoinCore.swift', '~> 0.11.0'
  s.dependency 'Hodler.swift', '~> 0.11.0'
  s.dependency 'OpenSslKit.swift', '~> 1.0'
  s.dependency 'Secp256k1Kit.swift', '~> 1.0'
  s.dependency 'HSHDWalletKit', '~> 1.3'

  s.dependency 'Alamofire', '~> 4.0'
  s.dependency 'ObjectMapper', '~> 3.0'
  s.dependency 'RxSwift', '~> 5.0'
  s.dependency 'BigInt', '~> 4.0'
  s.dependency 'GRDB.swift', '~> 4.0'
end
