//
//  REEntityCollection.swift
//  RxEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import RxSwift

struct REWeakObjectObservable<Entity: REEntity>
{
    weak var ref: REEntityObservable<Entity>?
}

public class REEntityCollection<Entity: REEntity>
{
    var items = [REWeakObjectObservable<Entity>]()
    var sharedEntities = [REEntityKey: Entity]()
    
    public var entitiesByKey: [REEntityKey: Entity] { sharedEntities }
    
    let lock = NSRecursiveLock()
    let queue: SchedulerType
    let dispBag = DisposeBag()
    
    init( queue: SchedulerType )
    {
        self.queue = queue
    }
    
    func Add( object: REEntityObservable<Entity> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        items.append( REWeakObjectObservable( ref: object ) )
    }
    
    func Remove( object: REEntityObservable<Entity> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        items.removeAll( where: { object.uuid == $0.ref?.uuid } )
    }
    
    func RxRequestForCombine( source: String = "", entity: Entity, updateChilds: Bool = true ) -> Single<Entity>
    {
        preconditionFailure( "" )
    }
    
    func RxRequestForCombine( source: String = "", entities: [Entity], updateChilds: Bool = true ) -> Single<[Entity]>
    {
        preconditionFailure( "" )
    }
    
    public func RxUpdate( source: String = "", entity: Entity ) -> Single<Entity>
    {
        return Single.create
            {
                [weak self] in
                
                self?.Update( source: source, entity: entity )
                $0( .success( entity ) )
                
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
    }
    
    public func RxUpdate( source: String = "", entities: [Entity] ) -> Single<[Entity]>
    {
        Update( source: source, entities: entities )
        return Single.create
            {
                [weak self] in
                
                self?.Update( source: source, entities: entities )
                $0( .success( entities ) )
                
                return Disposables.create()
            }
            .observe( on: queue )
            .subscribe( on: queue )
    }
    
    open func Update( source: String = "", entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        sharedEntities[entity._key] = entity
        items.forEach { $0.ref?.Update( source: source, entity: entity ) }
    }
    
    open func Update( source: String = "", entities: [Entity] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        entities.forEach { sharedEntities[$0._key] = $0 }
        items.forEach { $0.ref?.Update( source: source, entities: entities.asEntitiesMap() ) }
    }
    
    //MARK: - Commit
    public func Commit( entity: Entity, operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entity: REBackEntityProtocol, operation: REUpdateOperation = .update )
    {
        Commit( entity: Entity( entity: entity ), operation: operation )
    }
    
    public func Commit( key: REEntityKey, operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( key: REEntityKey, changes: (Entity) -> Entity )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [Entity], operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [REBackEntityProtocol], operation: REUpdateOperation = .update )
    {
        Commit( entities: entities.map { Entity( entity: $0 ) }, operation: operation )
    }
    
    public func Commit( entities: [Entity], operations: [REUpdateOperation] )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [REBackEntityProtocol], operations: [REUpdateOperation] )
    {
        Commit( entities: entities.map { Entity( entity: $0 ) }, operations: operations )
    }
    
    public func Commit( keys: [REEntityKey], operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( keys: [REEntityKey], operations: [REUpdateOperation] )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( keys: [REEntityKey], changes: (Entity) -> Entity )
    {
        fatalError( "This method must be overridden" )
    }
    
    func CommitDelete( keys: Set<REEntityKey> )
    {
        fatalError( "This method must be overridden" )
    }
    
    func CommitClear()
    {
        fatalError( "This method must be overridden" )
    }
    
    //MARK: - Create
    func CreateSingle( initial: Entity, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        fatalError( "This method must be overridden" )
    }

    func CreateKeyArray( initial: [Entity] ) -> REKeyArrayObservable<Entity>
    {
        fatalError( "This method must be overridden" )
    }
}
