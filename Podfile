source 'https://github.com/CocoaPods/Specs.git'
platform :ios, '12.0'
use_frameworks!

def shared_pods
  pod 'RxSwift',    '~> 6.10.1'
  pod 'RxCocoa',    '~> 6.10.1'
  pod 'RxRelay',    '~> 6.10.1'
end

target 'RxEntity' do
    shared_pods
end

target 'RxEntityTests' do
    shared_pods
    
    pod 'RxBlocking', '~> 6.10.1'
    pod 'RxTest', '~> 6.10.1'
end
