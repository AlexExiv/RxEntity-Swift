//
//  REEntityObservableCollection.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 25/11/2019.
//  Copyright © 2019 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import RxSwift
import RxRelay

public struct REEntityCollectionExtraParamsEmpty
{
    
}

typealias TestMethod<E> = (E, Array<Any>) -> Bool
typealias CombineMethod<E> = (E, Array<Any>) -> E

struct RECombineSource<E>
{
    let sources: [Observable<Any>]
    let test: TestMethod<E>
    let combine: CombineMethod<E>
}

/// Shared collection of entity obesrvales of particular `Entity` type that coordinates and manages observables updating
/// - Parameters:
///   - Entity: type of a `entity` managed by this collection
///   - CollectionExtra: `Extra` type that contains parameter or parameters that can cause process of reloading of all data related to this collection. This `Extra` passes to all fetch blocks and you can use it in all your requests
public class REEntityObservableCollectionExtra<Entity: REEntity, CollectionExtra>: REEntityCollection<Entity>
{
    public typealias SingleFetchBackCallback = RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.SingleFetchBackCallback
    public typealias SingleExtraFetchBackCallback<Extra> = RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>.SingleFetchBackCallback
    
    public typealias SingleFetchCallback = RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.SingleFetchCallback
    public typealias SingleExtraFetchCallback<Extra> = RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>.SingleFetchCallback
    
    public typealias KeyArrayFetchBackCallback = REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.ArrayFetchBackCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias KeyArrayExtraFetchBackCallback<Extra> = REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>.ArrayFetchBackCallback<Extra, CollectionExtra>
    
    public typealias KeyArrayFetchCallback = REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.ArrayFetchCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias KeyArrayExtraFetchCallback<Extra> = REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>.ArrayFetchCallback<Extra, CollectionExtra>
    
    public typealias PageFetchBackCallback = REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.PageFetchBackCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias PageExtraFetchBackCallback<Extra> = REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>.PageFetchBackCallback<Extra, CollectionExtra>
    
    public typealias PageFetchCallback = REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.PageFetchCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias PageExtraFetchCallback<Extra> = REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>.PageFetchCallback<Extra, CollectionExtra>
    
    public var repository: REEntityRepositoryProtocol?
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            _repository = newValue
            if let r = newValue
            {
                singleFetchBackCallback = { $0.key == nil ? Single.just( nil ) : r._RxGet( key: $0.key! ) }
                arrayFetchBackCallback = { r._RxGet( keys: $0.keys ) }
                
                if let ar = r as? REEntityAllRepositoryProtocol
                {
                    allArrayFetchCallback = { _ in ar._RxFetchAll() }
                }
                else
                {
                    allArrayFetchCallback = nil
                }
                
                
                repositoryDisp?.dispose()
                
                repositoryDisp = r
                    .rxEntitiesUpdated
                    .observe( on: queue )
                    .subscribe( onNext:
                    {
                        let keys = $0.filter { $0.entity == nil && $0.fieldPath == nil }
                        let entities = $0.filter { $0.entity != nil && $0.fieldPath == nil }
                        let indirect = $0.filter { $0.fieldPath != nil }
                            .map { k in self.sharedEntities.values.filter { REEntityKey( $0[keyPath: k.fieldPath!] as! AnyHashable ) == k.key }.map { $0._key } }.flatMap { $0 }

                        REEntityCollectionConfig.Log( "\(type( of: Entity.self )): Repository requested update: \($0)" )
                        REEntityCollectionConfig.Log( "\(type( of: Entity.self )): Total updates: KEYS - \(keys.count); ENTITIES - \(entities.count); INDIRECT - \(indirect.count)" )
                        
                        if keys.count == 1
                        {
                            self.Commit( key: keys[0].key, operation: keys[0].operation )
                        }
                        else if keys.count > 1
                        {
                            self.Commit( keys: keys.map { $0.key }, operations: keys.map { $0.operation } )
                        }
                        
                        if indirect.count == 1
                        {
                            self.Commit( key: indirect[0], operation: .update )
                        }
                        else if indirect.count > 1
                        {
                            self.Commit( keys: indirect, operation: .update )
                        }
                        
                        if entities.count > 0
                        {
                            self.Commit( entities: entities.map { $0.entity! }, operations: entities.map { $0.operation } )
                        }
                    } )
                
                dispBag.insert( repositoryDisp! )
            }
            else
            {
                singleFetchBackCallback = nil
                arrayFetchBackCallback = nil
                allArrayFetchCallback = nil
            }
        }
        get
        {
            lock.lock()
            defer { lock.unlock() }
            
            return _repository
        }
    }
    var _repository: REEntityRepositoryProtocol? = nil
    var repositoryDisp: Disposable? = nil
    
    public var singleFetchBackCallback: SingleFetchBackCallback? = nil
    public var singleFetchCallback: SingleFetchCallback? = nil
    public var arrayFetchBackCallback: KeyArrayFetchBackCallback? = nil
    public var arrayFetchCallback: KeyArrayFetchCallback? = nil
    var allArrayFetchCallback: PageFetchBackCallback? = nil
    
    public private(set) var collectionExtra: CollectionExtra? = nil
    private(set) var combineSources = [RECombineSource<Entity>]()
    
    var combineDisp: Disposable? = nil
    
    public init( queue: DispatchQueue? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.collectionExtra = collectionExtra
        super.init( queue: SerialDispatchQueueScheduler( queue: queue ?? DispatchQueue( label: "observable.collection" ), internalSerialQueueName: "observable.collection" ) )
    }

    public convenience init( operationQueue: OperationQueue, collectionExtra: CollectionExtra? = nil )
    {
        self.init( collectionExtra: collectionExtra )
    }
    
    public convenience init( queue: OperationQueueScheduler, collectionExtra: CollectionExtra? = nil )
    {
        self.init( operationQueue: queue.operationQueue, collectionExtra: collectionExtra )
    }
    
    deinit
    {
        combineDisp?.dispose()
    }

    //MARK: - Single Observables
    /// Creates the `SingleObservable` connected to this collection
    /// - Parameters:
    ///   - key: start key of the single observable. Default is `nil`
    ///   - start: flag indicates start or not fetching fata immidiately after the object `init`. Default is `true`
    ///   - fetch: closure for the fetch request
    /// - Returns: single observable
    public func CreateSingleBack( key: REEntityKey? = nil, start: Bool = true, _ fetch: @escaping SingleFetchBackCallback ) -> RESingleObservable<Entity>
    {
        return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, key: key, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }

    public func CreateSingleBackExtra<Extra>( key: REEntityKey? = nil, extra: Extra? = nil, start: Bool = true, _ fetch: @escaping SingleExtraFetchBackCallback<Extra> ) -> RESingleObservableExtra<Entity, Extra>
    {
        return RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateSingle( key: REEntityKey? = nil, start: Bool = true, _ fetch: @escaping SingleFetchCallback ) -> RESingleObservable<Entity>
    {
        return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, key: key, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }

    public func CreateSingleExtra<Extra>( key: REEntityKey? = nil, extra: Extra? = nil, start: Bool = true, _ fetch: @escaping SingleExtraFetchCallback<Extra> ) -> RESingleObservableExtra<Entity, Extra>
    {
        return RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public override func CreateSingle( initial: Entity, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        if singleFetchCallback != nil
        {
            return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: queue, fetch: singleFetchCallback! )
        }
        else if singleFetchBackCallback != nil
        {
            return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: queue, fetch: singleFetchBackCallback! )
        }
        
        preconditionFailure( "To create Single with initial value you must specify singleFetchCallback or singleFetchBackCallback before" )
    }
    
    public func CreateSingle( key: REEntityKey? = nil, start: Bool = true, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        let e = key == nil ? nil : sharedEntities[key!]
        if e == nil
        {
            if singleFetchCallback != nil
            {
                return CreateSingle( key: key, start: key != nil && start, singleFetchCallback! )
            }
            else if singleFetchBackCallback != nil
            {
                return CreateSingleBack( key: key, start: key != nil && start, singleFetchBackCallback! )
            }
            
            preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
        }
        
        return CreateSingle( initial: e!, refresh: refresh )
    }
    
    //MARK: - Array Observables
    public func CreateArrayBack( start: Bool = true, _ fetch: @escaping PageFetchBackCallback ) -> REArrayObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayBackExtra<Extra>( extra: Extra? = nil, start: Bool = true, _ fetch: @escaping PageExtraFetchBackCallback<Extra> ) -> REArrayObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayBack() -> REArrayObservable<Entity>
    {
        if let af = allArrayFetchCallback
        {
            return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: true, observeOn: queue, fetch: af )
        }
        
        preconditionFailure( "Repository doesn't conform to REEntityAllRepositoryProtocol" )
    }
    
    public func CreateArray( start: Bool = true, _ fetch: @escaping PageFetchCallback ) -> REArrayObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayExtra<Extra>( extra: Extra? = nil, start: Bool = true, _ fetch: @escaping PageExtraFetchCallback<Extra> ) -> REArrayObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    //MARK: - Array Keys Observables
    public func CreateKeyArrayBack( keys: [REEntityKey] = [], _ fetch: @escaping KeyArrayFetchBackCallback ) -> REKeyArrayObservable<Entity>
    {
        return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, keys: keys, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArrayBackExtra<Extra>( keys: [REEntityKey] = [], extra: Extra? = nil, _ fetch: @escaping KeyArrayExtraFetchBackCallback<Extra> ) -> REKeyArrayObservableExtra<Entity, Extra>
    {
        return REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArray( keys: [REEntityKey] = [], _ fetch: @escaping KeyArrayFetchCallback ) -> REKeyArrayObservable<Entity>
    {
        return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, keys: keys, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArrayExtra<Extra>( keys: [REEntityKey] = [], extra: Extra? = nil, _ fetch: @escaping KeyArrayExtraFetchCallback<Extra> ) -> REKeyArrayObservableExtra<Entity, Extra>
    {
        return REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public override func CreateKeyArray( initial: [Entity] ) -> REKeyArrayObservable<Entity>
    {
        if arrayFetchCallback != nil
        {
            return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, collectionExtra: collectionExtra, observeOn: queue, fetch: arrayFetchCallback! )
        }
        else if arrayFetchBackCallback != nil
        {
            return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, collectionExtra: collectionExtra, observeOn: queue, fetch: arrayFetchBackCallback! )
        }
        
        preconditionFailure( "To create Array with initial values you must specify arrayFetchCallback or arrayFetchBackCallback before" )
    }
    
    public func CreateKeyArray( keys: [REEntityKey] ) -> REKeyArrayObservable<Entity>
    {
        if arrayFetchCallback != nil
        {
            return CreateKeyArray( keys: keys, arrayFetchCallback! )
        }
        else if arrayFetchBackCallback != nil
        {
            return CreateKeyArrayBack( keys: keys, arrayFetchBackCallback! )
        }
        
        preconditionFailure( "To create Array with initial values you must specify arrayFetchCallback or arrayFetchBackCallback before" )
    }
    
    //MARK: - Paginator Observables
    public func CreatePaginatorBack( perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageFetchBackCallback ) -> REPaginatorObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginatorBackExtra<Extra>( extra: Extra? = nil, perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageExtraFetchBackCallback<Extra> ) -> REPaginatorObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginator( perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageFetchCallback ) -> REPaginatorObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginatorExtra<Extra>( extra: Extra? = nil, perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageExtraFetchCallback<Extra> ) -> REPaginatorObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    //MARK: - Combine Latest
    public func combineLatest<O: ObservableType>( _ source: O, test: @escaping ((Entity, O.Element)) -> Bool = { _ in true }, apply: @escaping ((Entity, O.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        combineSources.append( RECombineSource<Entity>( sources: [source.map { $0 as Any }.observe( on: queue )], test: { (e, a) in test( (e, a[0] as! O.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O.Element) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: ObservableType, O1: ObservableType>( _ source0: O0, _ source1: O1, _ test: @escaping ((Entity, O0.Element, O1.Element)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Element, O1.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [source0.map { $0 as Any }.observe( on: queue ),
                       source1.map { $0 as Any }.observe( on: queue )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Element, a[1] as! O1.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Element, a[1] as! O1.Element) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: ObservableType, O1: ObservableType, O2: ObservableType>( _ source0: O0, _ source1: O1, _ source2: O2, test: @escaping ((Entity, O0.Element, O1.Element, O2.Element)) -> Bool = { _ in true }, _ apply: @escaping ((Entity, O0.Element, O1.Element, O2.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [source0.map { $0 as Any }.observe( on: queue ),
                       source1.map { $0 as Any }.observe( on: queue ),
                       source2.map { $0 as Any }.observe( on: queue )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: ObservableType, O1: ObservableType, O2: ObservableType, O3: ObservableType>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, test: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [source0.map { $0 as Any }.observe( on: queue ),
                       source1.map { $0 as Any }.observe( on: queue ),
                       source2.map { $0 as Any }.observe( on: queue ),
                       source3.map { $0 as Any }.observe( on: queue )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: ObservableType, O1: ObservableType, O2: ObservableType, O3: ObservableType, O4: ObservableType>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, _ source4: O4, test: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element, O4.Element)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element, O4.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [source0.map { $0 as Any }.observe( on: queue ),
                       source1.map { $0 as Any }.observe( on: queue ),
                       source2.map { $0 as Any }.observe( on: queue ),
                       source3.map { $0 as Any }.observe( on: queue ),
                       source4.map { $0 as Any }.observe( on: queue )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element, a[4] as! O4.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element, a[4] as! O4.Element) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: ObservableType, O1: ObservableType, O2: ObservableType, O3: ObservableType, O4: ObservableType, O5: ObservableType>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, _ source4: O4, _ source5: O5, _ test: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element, O4.Element, O5.Element)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Element, O1.Element, O2.Element, O3.Element, O4.Element, O5.Element)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [source0.map { $0 as Any }.observe( on: queue ),
                       source1.map { $0 as Any }.observe( on: queue ),
                       source2.map { $0 as Any }.observe( on: queue ),
                       source3.map { $0 as Any }.observe( on: queue ),
                       source4.map { $0 as Any }.observe( on: queue ),
                       source5.map { $0 as Any }.observe( on: queue )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element, a[4] as! O4.Element, a[5] as! O5.Element) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Element, a[1] as! O1.Element, a[2] as! O2.Element, a[3] as! O3.Element, a[4] as! O4.Element, a[5] as! O5.Element) ) } ) )
        BuildCombines()
    }
    
    override func RxRequestForCombine( source: String = "", entity: Entity, updateChilds: Bool = true ) -> Single<Entity>
    {
        lock.lock()
        defer { lock.unlock() }
        
        var obs = Observable.just( entity )
        combineSources.forEach {
            ms in
            switch ms.sources.count
            {
            case 1:
                obs = Observable.combineLatest( obs, ms.sources[0], resultSelector: { ms.combine( $0, [$1] ) } )
            case 2:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], resultSelector: { ms.combine( $0, [$1, $2] ) } )
            case 3:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], resultSelector: { ms.combine( $0, [$1, $2, $3] ) } )
            case 4:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], resultSelector: { ms.combine( $0, [$1, $2, $3, $4] ) } )
            case 5:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], resultSelector: { ms.combine( $0, [$1, $2, $3, $4, $5] ) } )
            case 6:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], ms.sources[6], resultSelector: { ms.combine( $0, [$1, $2, $3, $4, $5, $6] ) } )
            default:
                assert( false, "Unsupported number of the sources" )
            }
        }
        
        return obs
            .take( 1 )
            .do( onNext:
            {
                if updateChilds
                {
                    self.Update( source: source, entity: $0 )
                }
            } )
            .asSingle()
    }
    
    override func RxRequestForCombine( source: String = "", entities: [Entity], updateChilds: Bool = true ) -> Single<[Entity]>
    {
        lock.lock()
        defer { lock.unlock() }
        
        var obs = Observable.just( entities )
        combineSources.forEach {
            ms in
            switch ms.sources.count
            {
            case 1:
                obs = Observable.combineLatest( obs, ms.sources[0], resultSelector: { (es, t0) in es.map { ms.combine( $0, [t0] ) } } )
            case 2:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], resultSelector: { (es, t0, t1) in es.map { ms.combine( $0, [t0, t1] ) } } )
            case 3:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], resultSelector: { (es, t0, t1, t2) in es.map { ms.combine( $0, [t0, t1, t2] ) } } )
            case 4:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], resultSelector: { (es, t0, t1, t2, t3) in es.map { ms.combine( $0, [t0, t1, t2, t3] ) } } )
            case 5:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], resultSelector: { (es, t0, t1, t2, t3, t4) in es.map { ms.combine( $0, [t0, t1, t2, t3, t4] ) } } )
            case 6:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], ms.sources[5], resultSelector: { (es, t0, t1, t2, t3, t4, t5) in es.map { ms.combine( $0, [t0, t1, t2, t3, t4, t5] ) } } )
            default:
                assert( false, "Unsupported number of the sources" )
            }
        }
        
        return obs
            .take( 1 )
            .do( onNext:
            {
                if updateChilds
                {
                    self.Update( source: source, entities: $0 )
                }
            } )
            .asSingle()
    }
    
    func BuildCombines()
    {
        var obs: Observable<[(test: TestMethod<Entity>, combine: CombineMethod<Entity>, values: [Any])]>! = Observable.just( [] ).observe( on: queue )
        combineSources.forEach {
            ms in
            switch ms.sources.count
            {
            case 1:
                obs = Observable.combineLatest( obs, ms.sources[0], resultSelector: { (arr, t0) in  arr + [(ms.test, ms.combine, [t0])] } )
            case 2:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], resultSelector: { (arr, t0, t1) in arr + [(ms.test, ms.combine, [t0, t1])] } )
            case 3:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], resultSelector: { (arr, t0, t1, t2) in arr + [(ms.test, ms.combine, [t0, t1, t2])] } )
            case 4:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], resultSelector: { (arr, t0, t1, t2, t3) in arr + [(ms.test, ms.combine, [t0, t1, t2, t3])] } )
            case 5:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], resultSelector: { (arr, t0, t1, t2, t3, t4) in arr + [(ms.test, ms.combine, [t0, t1, t2, t3, t4])] } )
            case 6:
                obs = Observable.combineLatest( obs, ms.sources[0], ms.sources[1], ms.sources[2], ms.sources[3], ms.sources[4], ms.sources[5], resultSelector: { (arr, t0, t1, t2, t3, t4, t5) in arr + [(ms.test, ms.combine, [t0, t1, t2, t3, t4, t5])] } )
            default:
                assert( false, "Unsupported number of the sources" )
            }
        }
        
        combineDisp?.dispose()
        weak var _self = self
        combineDisp = obs
            .subscribe( onNext: { _self?.ApplyCombines( combines: $0 ) } )
    }
    
    func ApplyCombines( combines: [(test: TestMethod<Entity>, combine: CombineMethod<Entity>, values: [Any])] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        var toUpdate = [REEntityKey: Entity]()
        /*for c in combines
        {
            if c.values.count == 1, let map = c.values[0] as? [REEntityKey: Any]
            {
                for k in map.keys
                {
                    var e = sharedEntities[k]!
                    if c.test( e, c.values )
                    {
                        e = c.combine( e, c.values )
                        sharedEntities[k] = e
                        toUpdate[k] = e
                    }
                }
            }
            else
            {
                for k in sharedEntities.keys
                {
                    var e = sharedEntities[k]!
                    if c.test( e, c.values )
                    {
                        e = c.combine( e, c.values )
                        sharedEntities[k] = e
                        toUpdate[k] = e
                    }
                }
            }
            
            
        }*/
        
        sharedEntities.keys.forEach
        {
            var e = sharedEntities[$0]!
            var updated = false
            for c in combines
            {
                if c.test( e, c.values )
                {
                    updated = true
                    e = c.combine( e, c.values )
                }
            }
            
            if updated
            {
                sharedEntities[$0] = e
                toUpdate[$0] = e
            }
        }
        
        if !toUpdate.isEmpty
        {
            items.forEach { $0.ref?.Update( source: "", entities: toUpdate ) }
        }
    }
    
    //MARK: - Commit
    public override func Commit( entity: Entity, operation: REUpdateOperation = .update )
    {
        Commit( entities: [entity], operation: operation )
    }

    public override func Commit( key: REEntityKey, operation: REUpdateOperation )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( arrayLiteral: key ) )
            
        case .clear:
            CommitClear()
            
        default:
            if let r = repository
            {
                r._RxGet( key: key )
                    .observe( on: queue )
                    .flatMap { $0 == nil ? Single.just( nil ) : self.RxRequestForCombine( source: "", entity: Entity( entity: $0! ), updateChilds: false ).map { $0 } }
                    .subscribe( onSuccess:
                    {
                        if let e = $0
                        {
                            self.Commit( entity: e, operation: operation )
                        }
                    }, onFailure: { _ in } )
                    .disposed( by: dispBag )
            }
            else
            {
                preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
            }
        }
    }
    
    public override func Commit( key: REEntityKey, changes: (Entity) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        if let e = sharedEntities[key]
        {
            let new = changes( e )
            sharedEntities[key] = new
            items.forEach { $0.ref?.Update( entity: new, operation: .update ) }
        }
    }
    
    public override func Commit( entities: [Entity], operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( entities.map { $0._key } ) )
            
        case .clear:
            CommitClear()
            
        default:
            RxRequestForCombine( entities: entities, updateChilds: false )
                .subscribe( on: queue )
                .observe( on: queue )
                .subscribe(onSuccess:
                {
                    enities in
                    
                    self.lock.lock()
                    defer { self.lock.unlock() }
                    
                    var forUpdate = [REEntityKey: Entity]()
                    enities.forEach
                    {
                        forUpdate[$0._key] = $0
                        self.sharedEntities[$0._key] = $0
                    }
                    
                    entities.forEach { self.sharedEntities[$0._key] = $0 }
                    self.items.forEach { $0.ref?.Update( entities: forUpdate, operation: operation ) }
                } )
                .disposed( by: dispBag )
        }
    }
    
    public override func Commit( entities: [Entity], operations: [REUpdateOperation] )
    {
        let deleteKeys = Set( entities.enumerated().filter { operations[$0.0] == .delete }.map { $0.1._key } )
        let otherEntities = entities.enumerated().filter { operations[$0.0] != .delete }.map { $0.1 }
        let otherOpers = operations.filter { $0 != .delete }
        
        if let _ = operations.first( where: { $0 == .clear } )
        {
            CommitClear()
        }
        
        CommitDelete( keys: deleteKeys )

        RxRequestForCombine( entities: otherEntities, updateChilds: false )
            .subscribe( on: queue )
            .observe( on: queue )
            .subscribe( onSuccess:
            {
                enities in
                
                self.lock.lock()
                defer { self.lock.unlock() }
                
                var forUpdate = [REEntityKey: Entity]()
                var operationUpdate = [REEntityKey: REUpdateOperation]()
                enities.enumerated().forEach
                {
                    forUpdate[$1._key] = $1
                    operationUpdate[$1._key] = otherOpers[$0]
                    self.sharedEntities[$1._key] = $1
                }
                
                entities.forEach { self.sharedEntities[$0._key] = $0 }
                self.items.forEach { $0.ref?.Update( entities: forUpdate, operations: operationUpdate ) }
            } )
            .disposed( by: dispBag )
    }
    
    public override func Commit( keys: [REEntityKey], operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( keys ) )
            
        case .clear:
            CommitClear()
            
        default:
            if let r = repository
            {
                r._RxGet( keys: keys )
                    .observe( on: queue )
                    .flatMap { self.RxRequestForCombine( source: "", entities: $0.map { Entity( entity: $0 ) }, updateChilds: false ).map { $0 } }
                    .subscribe( onSuccess: { self.Commit( entities: $0, operation: operation ) }, onFailure: { _ in } )
                    .disposed( by: dispBag )
            }
            else
            {
                preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
            }
        }
    }
    
    public override func Commit( keys: [REEntityKey], operations: [REUpdateOperation] )
    {
        let deleteKeys = Set( keys.enumerated().filter { operations[$0.0] == .delete }.map { $0.1 } )
        let otherKeys = keys.enumerated().filter { operations[$0.0] != .delete }.map { $0.1 }
        let otherOpers = operations.filter { $0 != .delete }
        
        if let _ = operations.first( where: { $0 == .clear } )
        {
            CommitClear()
        }
        
        CommitDelete( keys: deleteKeys )
        
        if let r = repository
        {
            r._RxGet( keys: otherKeys )
                .observe( on: queue )
                .flatMap { self.RxRequestForCombine( source: "", entities: $0.map { Entity( entity: $0 ) }, updateChilds: false ).map { $0 } }
                .subscribe( onSuccess: { self.Commit( entities: $0, operations: otherOpers ) }, onFailure: { _ in } )
                .disposed( by: dispBag )
        }
        else
        {
            preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
        }
    }
    
    public override func Commit( keys: [REEntityKey], changes: (Entity) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        var forUpdate = [REEntityKey: Entity]()
        keys.forEach
        {
            if let e = sharedEntities[$0]
            {
                let new = changes( e )
                sharedEntities[$0] = new
                forUpdate[$0] = new
            }
        }
        
        items.forEach { $0.ref?.Update( entities: forUpdate, operation: .update ) }
    }
    
    override func CommitDelete( keys: Set<REEntityKey> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        items.forEach { $0.ref?.Delete( keys: keys ) }
    }
    
    override func CommitClear()
    {
        lock.lock()
        defer { lock.unlock() }
        
        sharedEntities = [:]
        items.forEach { $0.ref?.Clear() }
    }

    //MARK: - Updates
    public func RxRequestForUpdate( source: String = "", key: REEntityKey, update: @escaping (Entity) -> Entity ) -> Single<Entity?>
    {
        return Single.create
            {
                [weak self] in
                
                if let entity = self?.sharedEntities[key]
                {
                    let new = update( entity )
                    self?.Update( source: source, entity: update( entity ) )
                    $0( .success( new ) )
                }
                else
                {
                    $0( .success( nil ) )
                }
                
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
    }
    
    public func RxRequestForUpdate( source: String = "", keys: [REEntityKey], update: @escaping (Entity) -> Entity ) -> Single<[Entity]>
    {
        return Single.create
            {
                [weak self] in
                
                var updArr = [Entity](), updMap = [REEntityKey: Entity]()
                keys.forEach
                {
                    if let entity = self?.sharedEntities[$0]
                    {
                        let new = update( entity )
                        self?.sharedEntities[$0] = new
                        updArr.append( new )
                        updMap[$0] = new
                    }
                }
                
                self?.items.forEach { $0.ref?.Update( source: source, entities: updMap ) }
                $0( .success( updArr ) )
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
    }
    
    public func RxRequestForUpdate( source: String = "", update: @escaping (Entity) -> Entity ) -> Single<[Entity]>
    {
        return RxRequestForUpdate( source: source, keys: sharedEntities.keys.map { $0 }, update: update )
    }
    
    public func RxRequestForUpdate<EntityS: REEntity>( source: String = "", entities: [REEntityKey: EntityS], underPathes: [KeyPath<Entity, REEntity>], update: @escaping (Entity, EntityS) -> Entity ) -> Single<[Entity]>
    {
        return Single.create
            {
                [weak self] in
                
                var updArr = [Entity](), updMap = [REEntityKey: Entity]()
                let Update: (Entity, EntityS) -> Void = {
                    let new = update( $0, $1 )
                    self?.sharedEntities[$0._key] = new
                    updArr.append( new )
                    updMap[$0._key] = new
                }
                self?.sharedEntities.forEach
                {
                    e0 in
                    
                    underPathes.forEach
                    {
                        if let v = e0.value[keyPath: $0] as? EntityS, let es = entities[v._key]
                        {
                            Update( e0.value, es )
                        }
                        else if let arr = e0.value[keyPath: $0] as? [EntityS]
                        {
                            arr.forEach
                            {
                                e1 in
                                if let es = entities[e1._key]
                                {
                                    Update( e0.value, es )
                                }
                            }
                        }
                    }
                }
                
                self?.items.forEach { $0.ref?.Update( source: source, entities: updMap ) }
                $0( .success( updArr ) )
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
    }
    
    public func DispatchUpdates<EntityS: REEntity>( to: REEntityObservableCollectionExtra, withPathes: [KeyPath<EntityS, REEntity>] )
    {
        
    }
    
    public func DispatchUpdates<V>( to: REEntityObservableCollectionExtra, fromPathes: [KeyPath<Entity, V>], apply: (V) -> Entity )
    {
        
    }
    
    public func Refresh( resetCache: Bool = false, collectionExtra: CollectionExtra? = nil )
    {
        Single<Bool>.create
            {
                [weak self] in
                
                self?._Refresh( resetCache: resetCache, collectionExtra: collectionExtra )
                $0( .success( true ) )
                
                return Disposables.create()
            }
            .subscribe( on: queue )
            .observe( on: queue )
            .subscribe()
            .disposed( by: dispBag )
    }
    
    func _Refresh( resetCache: Bool = false, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        items.forEach { $0.ref?.RefreshData( resetCache: resetCache, data: self.collectionExtra ) }
    }
}
/*
public class ERZoombieRepostory<Entity: REEntity>: RERepository
{
    public typealias E = Entity
    
    public func RxGet( key: REEntityKey ) -> Single<Entity?>
    {
        return Single.just( nil )
    }
    
    public func RxGet( keys: REEntityKey ) -> Single<[Entity]>
    {
        return Single.just( [] )
    }
}
*/
public typealias REEntityObservableCollection<Entity: REEntity> = REEntityObservableCollectionExtra<Entity, REEntityCollectionExtraParamsEmpty>

//public typealias REEntityObservableCollectionExtra<Entity: REEntity, CollectionExtra> = REEntityObservableCollectionExtraRepository<Entity, CollectionExtra, ERZoombieRepostory<Entity>>

extension ObservableType
{
    public func bind<Entity: REEntity>( refresh: REEntityObservableCollectionExtra<Entity, Element>, resetCache: Bool = false ) -> Disposable
    {
        return observe( on: refresh.queue )
            .subscribe( onNext: { refresh._Refresh( resetCache: resetCache, collectionExtra: $0 ) } )
    }
}
