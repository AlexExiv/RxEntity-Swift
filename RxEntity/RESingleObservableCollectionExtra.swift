//
//  RESingleObservableService.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay

public struct RESingleParams<Entity: REEntity, Extra, CollectionExtra>
{
    public let refreshing: Bool
    public let resetCache: Bool
    public let first: Bool
    public let key: REEntityKey?
    public let lastEntity: Entity?
    public let extra: Extra?
    public let collectionExtra: CollectionExtra?
    
    init( refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, key: REEntityKey?, lastEntity: Entity?, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.key = key
        self.lastEntity = lastEntity
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

public class RESingleObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: RESingleObservableExtra<Entity, Extra>
{
    public typealias SingleFetchBackCallback = (RESingleParams<Entity, Extra, CollectionExtra>) -> Single<REBackEntityProtocol?>
    public typealias SingleFetchCallback = (RESingleParams<Entity, Extra, CollectionExtra>) -> Single<Entity?>
    
    let _rxRefresh = BehaviorRelay<RESingleParams<Entity, Extra, CollectionExtra>?>( value: nil )
    public private(set) var collectionExtra: CollectionExtra? = nil
    var started = false
    
    public override var key: REEntityKey?
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            super.key = newValue
            
            let params = _rxRefresh.value
            _rxRefresh.accept( RESingleParams( refreshing: true, resetCache: true, first: true, key: newValue, lastEntity: entity, extra: params?.extra, collectionExtra: params?.collectionExtra ) )
            started = true
        }
        get
        {
            super.key
        }
    }

    init( holder: REEntityCollection<Entity>, key: REEntityKey? = nil, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, start: Bool = true, observeOn: SchedulerType, fetch: @escaping SingleFetchCallback )
    {
        self.collectionExtra = collectionExtra
        
        super.init( holder: holder, key: key, extra: extra, observeOn: observeOn )
        
        weak var _self = self
        _rxRefresh
            .filter { $0 != nil }
            .map { $0! }
            .do( onNext:
            {
                _self?.rxLoader.accept( $0.first ? .firstLoading : .loading )
                if $0.first
                {
                    _self?.rxState.accept( .initializing )
                }
            } )
            .flatMapLatest
            {
                fetch( $0 )
                    .asObservable()
                    .flatMap
                    {
                        e -> Observable<Entity> in
                        
                        if e == nil
                        {
                            //_self?.rxState.accept( .notFound )
                            return Observable.error( NSError( domain: "", code: 404, userInfo: nil ) )
                        }
                        
                        return Observable.just( e! )
                    }
                    .catch
                    {
                        e -> Observable<Entity> in
                        if (e as NSError).code == 404
                        {
                            _self?.rxState.accept( .notFound )
                        }
                        else
                        {
                            _self?.rxError.accept( e )
                        }
                        _self?.rxLoader.accept( .none )
                        return Observable.empty()
                    }
            }
            .observe( on: observeOn )
            .do( onNext:
            {
                _ in
                //_self?.Set( key: $0._key )
                _self?.rxLoader.accept( .none )
                _self?.rxState.accept( .ready )
            } )
            .flatMap { _self?.collection?.RxRequestForCombine( source: _self?.uuid ?? "", entity: $0 ).map { $0 } ?? Single.just( nil ) }
            .bind( to: rxPublish )
            .disposed( by: dispBag )
        
        if start
        {
            started = true
            _rxRefresh.accept( RESingleParams( first: true, key: key, lastEntity: entity, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: Entity, refresh: Bool, collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping SingleFetchCallback )
    {
        self.init( holder: holder, key: initial._key, collectionExtra: collectionExtra, start: false, observeOn: observeOn, fetch: fetch )
        
        weak var _self = self
        Single.just( true )
            .observe( on: observeOn )
            .flatMap { _ in _self?.collection?.RxRequestForCombine( source: _self?.uuid ?? "", entity: initial ).map { $0 } ?? Single.just( nil ) }
            .subscribe( onSuccess:
            {
                _self?.rxPublish.onNext( $0 )
                _self?.rxState.accept( .ready )
            } )
            .disposed( by: dispBag )
        
        started = !refresh
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: Entity, refresh: Bool, collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping SingleFetchBackCallback )
    {
        self.init( holder: holder, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0 == nil ? nil : Entity( entity: $0! ) } } )
    }
    
    convenience init( holder: REEntityCollection<Entity>, key: REEntityKey? = nil, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, start: Bool = true, observeOn: SchedulerType,  fetch: @escaping SingleFetchBackCallback )
    {
        self.init( holder: holder, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: observeOn, fetch: { fetch( $0 ).map { $0 == nil ? nil : Entity( entity: $0! ) } } )
    }
    
    override func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    override func RefreshData( resetCache: Bool, data: Any? )
    {
        _CollectionRefresh( resetCache: resetCache, collectionExtra: data as? CollectionExtra )
    }
    
    func CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        Single<Bool>.create
            {
                [weak self] in
                
                self?._CollectionRefresh( resetCache: resetCache, extra: extra, collectionExtra: collectionExtra )
                $0( .success( true ) )
                
                return Disposables.create()
            }
            .subscribe( on: queue )
            .observe( on: queue )
            .subscribe()
            .disposed( by: dispBag )
    }
    
    public override func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    func _CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super._Refresh( resetCache: resetCache, extra: extra )
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        _rxRefresh.accept( RESingleParams( refreshing: true, resetCache: resetCache, first: !started, key: key, lastEntity: entity, extra: self.extra, collectionExtra: self.collectionExtra ) )
        started = true
    }
}
