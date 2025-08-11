//
//  REKeyArrayObservableCollectionExtra.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 07.10.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

import RxSwift
import RxRelay
import RxCocoa

public struct REKeyParams<Extra, CollectionExtra>
{
    /// Update requested from refreshing
    public let refreshing: Bool
    public let resetCache: Bool
    /// The first loading request flag
    public let first: Bool
    /// The keys of requested elements
    public let keys: [REEntityKey]
    /// The observable's extra params for example filter and e.g.
    public let extra: Extra?
    /// The collection extra params it maybe a region or city or so on
    public let collectionExtra: CollectionExtra?
    
    init( refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, keys: [REEntityKey], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.keys = keys
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

public class REKeyArrayObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: REKeyArrayObservableExtra<Entity, Extra>
{
    public typealias Element = [Entity]
    public typealias ArrayFetchCallback<Extra, CollectionExtra> = (REKeyParams<Extra, CollectionExtra>) -> Single<[Entity]>
    public typealias ArrayFetchBackCallback<Extra, CollectionExtra> = (REKeyParams<Extra, CollectionExtra>) -> Single<[REBackEntityProtocol]>
    
    public override var _keys: [REEntityKey]
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            super._keys = newValue
            rxKeys.accept( REKeyParams( keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
        get
        {
            super._keys
        }
    }
    
    let rxKeys = PublishRelay<REKeyParams<Extra, CollectionExtra>>()

    public private(set) var collectionExtra: CollectionExtra? = nil
      
    init( holder: REEntityCollection<Entity>, keys: [REEntityKey] = [], start: Bool = true, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> )
    {
        self.collectionExtra = collectionExtra
        super.init( holder: holder, keys: keys, extra: extra, observeOn: observeOn )
        
        weak var _self = self
        Observable
            .combineLatest( rxKeys, rxSuspended )
            .filter { $0.0.keys.count > 0 && !$0.1 }
            .map { $0.0 }
            .do( onNext:
            {
                _self?.rxLoader.accept( $0.first ? .firstLoading : .loading )
                _self?.rxLastError.accept( nil )
            } )
            .observe( on: queue )
            .flatMapLatest( {
                (_self?.RxFetchElements( params: $0, fetch: fetch ) ?? Single.just( [] ))
                    .asObservable()
                    .catch
                    {
                        _self?.rxError.accept( $0 )
                        _self?.rxLastError.accept( $0 )
                        _self?.rxLoader.accept( .none )
                        return Observable.just( [] )
                    }
            } )
                .observe( on: observeOn )
            .do( onNext: { _ in _self?.rxLoader.accept( .none ) } )
            .flatMap { _self?.collection?.RxRequestForCombine( source: _self?.uuid ?? "", entities: $0 ) ?? Single.just( [] ) }
            .subscribe( onNext: { _self?.Set( entities: $0 ) } )
            .disposed( by: dispBag )
        
        rxKeys
            .filter { $0.keys.count == 0 }
            .observe( on: observeOn )
            .do( onNext: { _ in _self?.rxLoader.accept( .none ) } )
            .subscribe( onNext: { _ in _self?.Set( entities: [] ) } )
            .disposed( by: dispBag )
        
        if start
        {
            rxKeys.accept( REKeyParams( first: true, keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, keys: initial.map { $0._key }, start: false, collectionExtra: collectionExtra, observeOn: observeOn, fetch: fetch )
        
        weak var _self = self
        Single.just( true )
            .observe( on: observeOn )
            .flatMap { _ in _self?.collection?.RxRequestForCombine( source: _self?.uuid ?? "", entities: initial ) ?? Single.just( [] ) }
            .subscribe( onSuccess: { _self?.Set( entities: $0 ) } )
            .disposed( by: dispBag )
    }
    
    convenience init( holder: REEntityCollection<Entity>, keys: [REEntityKey] = [], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping ArrayFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } } } )
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: SchedulerType, fetch: @escaping ArrayFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, initial: initial, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } } } )
    }

    public override func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    override func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra )
    }

    override func RefreshData( resetCache: Bool, data: Any? )
    {
        _CollectionRefresh( resetCache: resetCache, collectionExtra: data as? CollectionExtra )
    }
    
    //MARK: - Collection
    func CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        Single<Bool>.create
            {
                [weak self] in
                
                self?._CollectionRefresh( resetCache: resetCache, extra: extra, collectionExtra: collectionExtra )
                $0( .success( true ) )
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
            .subscribe()
            .disposed( by: dispBag )
    }
    
    func _CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super._Refresh( resetCache: resetCache, extra: extra )
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        rxKeys.accept( REKeyParams( refreshing: true, resetCache: resetCache, keys: keys, extra: self.extra, collectionExtra: self.collectionExtra ) )
    }
    
    //MARK: - Fetch
    private func RxFetchElements( params: REKeyParams<Extra, CollectionExtra>, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> ) -> Single<[Entity]>
    {
        if params.refreshing
        {
            return fetch( params )
        }
        
        let exist = params.keys.compactMap { k in collection?.sharedEntities[k] ?? entities.first( where: { k == $0._key } ) }
        if exist.count != keys.count
        {
            let _params = REKeyParams( refreshing: params.refreshing, resetCache: params.resetCache, first: params.first, keys: params.keys.filter { collection?.sharedEntities[$0] == nil }, extra: extra, collectionExtra: collectionExtra )
            return Observable
                .zip( Observable.just( exist ), fetch( _params ).asObservable() )
                .asSingle()
                .map
                {
                    let exist = $0.0.asEntitiesMap()
                    let new = $0.1.asEntitiesMap()
                    REEntityCollectionConfig.Log( "KeyArray exist: \(exist.count); new: \(new.count)" )
                    return params.keys.compactMap { exist[$0] ?? new[$0] }
                }
        }
        
        return Single.just( exist )
    }
}
