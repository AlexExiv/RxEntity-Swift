//
//  REPaginatorObservableCollectionExtra.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay
import RxCocoa

public let PAGINATOR_END = -1000

public struct REPageParams<Extra, CollectionExtra>
{
    public let page: Int
    public let perPage: Int
    public let refreshing: Bool
    public let resetCache: Bool
    public let first: Bool
    public let keys: [REEntityKey]
    public let extra: Extra?
    public let collectionExtra: CollectionExtra?
    
    init( page: Int, perPage: Int, refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, keys: [REEntityKey], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.page = page
        self.perPage = perPage
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.keys = keys
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

public class REPaginatorObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: REPaginatorObservableExtra<Entity, Extra>
{
    public typealias Element = [Entity]
    public typealias PageFetchCallback<Extra, CollectionExtra> = (REPageParams<Extra, CollectionExtra>) -> Single<Element>
    
    let rxPage = PublishRelay<REPageParams<Extra, CollectionExtra>>()

    public private(set) var collectionExtra: CollectionExtra? = nil
    var started = false
      
    init( holder: REEntityCollection<Entity>, keys: [REEntityKey] = [], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, perPage: Int = 35, start: Bool = true, observeOn: OperationQueueScheduler, fetch: @escaping PageFetchCallback<Extra, CollectionExtra> )
    {
        self.collectionExtra = collectionExtra
        super.init( holder: holder, keys: keys, extra: extra, perPage: perPage, observeOn: observeOn )
        
        weak var _self = self
        rxPage
            .filter { $0.page >= 0 }
            .do( onNext: { _ in _self?.rxLoader.accept( true ) } )
            .flatMapLatest( {
                fetch( $0 )
                    .asObservable()
                    .do( onNext: { _self?.Set( keys: $0.map { $0.key } ) } )
                    .catchError
                    {
                        _self?.rxError.accept( $0 )
                        return Observable.just( [] )
                    }
            } )
            .flatMap( { _self?.collection?.RxUpdate( source: _self?.uuid ?? "", entities: $0 ) ?? Single.just( [] ) } )
            .observeOn( observeOn )
            .map( { _self?.Append( entities: $0 ) ?? [] } )
            .do( onNext: { _ in _self?.rxLoader.accept( false ) } )
            .bind( to: rxPublish )
            .disposed( by: dispBag )

        if start
        {
            started = true
            rxPage.accept( REPageParams( page: 0, perPage: perPage, first: true, keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: OperationQueueScheduler, fetch: @escaping PageFetchCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, keys: initial.map { $0.key }, collectionExtra: collectionExtra, start: false, observeOn: observeOn, fetch: fetch )
        rxPublish.onNext( initial )
        started = true
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
    
    public override func Next()
    {
        if rxLoader.value
        {
            return
        }
        
        if started
        {
            rxPage.accept( REPageParams( page: page + 1, perPage: perPage, keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
        else
        {
            Refresh()
        }
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
            .observeOn( queue )
            .subscribeOn( queue )
            .subscribe()
            .disposed( by: dispBag )
    }
    
    func _CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        assert( queue.operationQueue == OperationQueue.current, "_Refresh can be updated only from the specified in the constructor OperationQueue" )
        
        super._Refresh( resetCache: resetCache, extra: extra )
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        rxPage.accept( REPageParams( page: page + 1, perPage: perPage, refreshing: true, resetCache: resetCache, first: !started, keys: keys, extra: self.extra, collectionExtra: self.collectionExtra ) )
        started = true
    }
}
