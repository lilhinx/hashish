import Foundation
import Combine
import SwiftProtobuf
import os.log



public protocol Collection:Hashable,CustomStringConvertible,RawRepresentable,CaseIterable
{
    var isOptimisticLockable:Bool{ get }
    func decode( data:Data )->Message?
    func decode( metadata:Data )->Message?
}

public protocol OptimisticLockable
{
    var version:Int64{ get }
}

public struct HashishValue
{
    public var data:Message
    public var metadata:Message?
}

public class HashishTable<CollectionType,KeyType> where CollectionType:Collection, KeyType:Hashable
{
    public typealias KeyValueStore = [KeyType:HashishValue]
    typealias KeyValueCollectionSubject = CurrentValueSubject<KeyValueStore,Never>
    public typealias KeyValueCollectionPublisher = AnyPublisher<KeyValueStore,Never>
    public typealias ReadTransactionBlock = ( KeyValueStore )->Void
    public typealias WriteTransactionBlock = ( KeyValueStore, inout WriteTransaction )->Void
    
    enum Phase
    {
        case prepare
        case commit
    }
    
    fileprivate static func put( data:Message, for key:KeyType, in collection:CollectionType, with store: inout KeyValueStore, phase:Phase, clobber:Bool = false )
    {
        if store.keys.contains( key )
        {
            var value = store[ key ]!
            if collection.isOptimisticLockable, let newLockable = data as? OptimisticLockable, let oldLockable = value.data as? OptimisticLockable
            {
                
                if newLockable.version > oldLockable.version || clobber
                {
                    value.data = data
                    store[ key ] = value
                }
            }
            else
            {
                value.data = data
                store[ key ] = value
            }
        }
        else
        {
            store[ key ] = .init( data:data, metadata:nil )
        }
    }
    
    fileprivate static func remove( for key:KeyType, in collection:CollectionType, with store: inout KeyValueStore )
    {
        store.removeValue( forKey:key )
    }
    
    fileprivate static func putMeta( metadata:Message, for key:KeyType, in collection:CollectionType, with store:inout KeyValueStore, initiallyOnly:Bool )
    {
        guard store.keys.contains( key ) else
        {
            return
        }
        
        if initiallyOnly
        {
            guard store[ key ]!.metadata == nil else
            {
                return
            }
        }
        
        store[ key ]!.metadata = metadata
    }
    
    fileprivate static func removeMeta( for key:KeyType, in collection:CollectionType, with store:inout KeyValueStore )
    {
        guard store.keys.contains( key ) else
        {
            return
        }

        store[ key ]!.metadata = nil
    }
    
    public struct WriteTransaction
    {
        internal let collection:CollectionType
        internal let existingKeys:Set<KeyType>
        internal var update:KeyValueStore = [ : ]
        internal var deletes:Set<KeyType> = [ ]
        internal var metadataUpdate:[KeyType:Message] = [ : ]
        internal var metadataUpdateIsInitiallyOnly:[KeyType:Bool] = [ : ]
        internal var metadataDeletes:Set<KeyType> = [ ]
        
        internal var mutationCount:Int
        {
            return update.count + deletes.count + metadataUpdate.count + metadataDeletes.count
        }
        
        internal var isMutated:Bool
        {
            return mutationCount > 0
        }
        
        fileprivate func process( using input:KeyValueStore )->KeyValueStore
        {
            var store = input
            for ( key, value ) in update
            {
                HashishTable.put( data:value.data, for:key, in:collection, with:&store, phase:.commit )
            }
            for delete in deletes
            {
                HashishTable.remove(for:delete, in:collection, with: &store )
            }
            for ( key, metadata ) in metadataUpdate
            {
                let initiallyOnly = metadataUpdateIsInitiallyOnly[ key ]!
                HashishTable.putMeta( metadata:metadata, for:key, in:collection, with:&store, initiallyOnly:initiallyOnly )
            }
            for delete in metadataDeletes
            {
                HashishTable.removeMeta( for:delete, in:collection, with:&store )
            }
            return store
        }
        
        public mutating func put( data:Message, for key:KeyType, clobber:Bool = false )
        {
            HashishTable.put( data:data, for:key, in:collection, with:&update, phase:.prepare, clobber:clobber )
            deletes.remove( key )
        }
        
        public mutating func remove( for key:KeyType )
        {
            HashishTable.remove( for:key, in:collection, with:&update )
            deletes.insert( key )
        }
                
        public mutating func putMeta( metadata:Message, for key:KeyType, initiallyOnly:Bool = false )
        {
            metadataUpdateIsInitiallyOnly[ key ] = initiallyOnly
            metadataUpdate[ key ] = metadata
            metadataDeletes.remove( key )
        }
        
        public mutating func removeMeta( for key:KeyType )
        {
            metadataUpdate.removeValue( forKey:key )
            metadataDeletes.insert( key )
        }
        
        public mutating func removeAll( notPresentIn keys:Set<KeyType> )
        {
            let deletes:Set<KeyType> = existingKeys.subtracting( keys )
            for key in deletes
            {
                remove( for:key )
            }
        }
    }
    
    let queue:DispatchQueue
    let diskQueue:DispatchQueue
    let log:OSLog
    let partition:String
    public init( label:String, partition:String )
    {
        queue = .init( label:label )
        diskQueue = .init( label:"\( label )_disk" )
        log = OSLog.init( subsystem:"Hashish", category:label )
        self.partition = partition
    }
    
   
    var subjects:[CollectionType:KeyValueCollectionSubject] = [ : ]
    
    private func getSubject( for collection:CollectionType )->KeyValueCollectionSubject
    {
        if !subjects.keys.contains( collection )
        {
            subjects[ collection ] = KeyValueCollectionSubject.init( [ : ] )
        }
        return subjects[ collection ]!
    }
    
    
    
    public func publisher( for collection:CollectionType )->KeyValueCollectionPublisher
    {
        return getSubject( for:collection ).eraseToAnyPublisher( )
    }
    
    
    public func read( collection:CollectionType, with block:@escaping ReadTransactionBlock )
    {
        queue.async
        {
            os_log( "read: %{public}@", log:self.log, type:.default, collection.description )
            let subject = self.getSubject( for:collection )
            block( subject.value )
        }
    }
    
    public func readWrite( collection:CollectionType, with block:@escaping WriteTransactionBlock )
    {
        queue.async
        {
            let subject = self.getSubject( for:collection )
            var transaction:WriteTransaction = .init( collection:collection, existingKeys:Set<KeyType>( subject.value.keys ) )
            block( subject.value, &transaction )
            if transaction.isMutated
            {
                os_log( "write: %{public}@", log:self.log, type:.default, collection.description )
                let mutatedValue = transaction.process( using:subject.value )
                subject.value = mutatedValue
                guard self.restored else
                {
                    return
                }
                
                self.diskQueue.async
                {
                    let serializedData:[KeyType:Data] = mutatedValue.compactMapValues
                    {
                        ( value )->Data? in
                        guard let data = try? value.data.serializedData( ) else
                        {
                            return nil
                        }
                        
                        guard data.count > 0 else
                        {
                            return nil
                        }
                        
                        return data
                    }
                    
                    do
                    {
                        let data = try NSKeyedArchiver.archivedData( withRootObject:serializedData, requiringSecureCoding:true )
                        try data.write( to:self.dataStorageURL( for:collection ) )
                        
                        let serializedMetadata:[KeyType:Data] = mutatedValue.compactMapValues
                        {
                            ( value )->Data? in
                            guard let data = try? value.metadata?.serializedData( ) else
                            {
                                return nil
                            }
                            
                            guard data.count > 0 else
                            {
                                return nil
                            }
                            
                            return data
                        }
                        
                        let metadata = try NSKeyedArchiver.archivedData( withRootObject:serializedMetadata, requiringSecureCoding:true )
                        try metadata.write( to:self.metadataStorageURL( for:collection ) )
                        os_log( "diskwrite: %{public}@", log:self.log, type:.default, collection.description )
                    }
                    catch
                    {
                        os_log( "disk write error: %{public}@", log:self.log, type:.error, error.localizedDescription )
                    }
                }
            }
        }
    }
    
    
    
    static func cacheDirectory( )->URL
    {
        guard let path = NSSearchPathForDirectoriesInDomains( .cachesDirectory, .userDomainMask, true ).first else
        {
            return URL.init( fileURLWithPath:"~/Library/Caches" )
        }
        return URL.init( fileURLWithPath:path )
    }
    
    private func storageDirectory( )->URL
    {
        let dir = Self.cacheDirectory( ).appendingPathComponent( "hashish" ).appendingPathComponent( partition )
        try! FileManager.default.createDirectory( at:dir, withIntermediateDirectories:true, attributes:[ : ] )
        return dir
    }
    
    private func metadataStorageURL( for collection:CollectionType )->URL
    {
        return storageDirectory( ).appendingPathComponent( "\( collection.description ).metadata" ).appendingPathExtension( "hashish" )
    }
    
    private func dataStorageURL( for collection:CollectionType )->URL
    {
        return storageDirectory( ).appendingPathComponent( collection.description ).appendingPathExtension( "hashish" )
    }
    
    var restored:Bool = false
    public func restore( then:( ( )->Void )? )
    {
        diskQueue.async
        {
            os_log( "restoring from disk", log:self.log, type:.default )
            for collection in CollectionType.allCases
            {
                guard let data = try? Data.init( contentsOf:self.dataStorageURL( for:collection ) ) else
                {
                    continue
                }
                
                do
                {
                    if let deserializedData = try NSKeyedUnarchiver.unarchiveTopLevelObjectWithData( data ) as? [KeyType:Data]
                    {
                        let dataValues = deserializedData.compactMapValues
                        {
                            ( data )->Message? in
                            return collection.decode( data:data )
                        }
                        
                        var metadataValues:[KeyType:Message] = [ : ]
                        
                        if let metadata = try? Data.init( contentsOf:self.metadataStorageURL( for:collection ) )
                        {
                            if let deserializedMetadata = try NSKeyedUnarchiver.unarchiveTopLevelObjectWithData( metadata ) as? [KeyType:Data]
                            {
                                metadataValues = deserializedMetadata.compactMapValues
                                {
                                    ( data )->Message? in
                                    return collection.decode( metadata:data )
                                }
                            }
                        }
                        
                        var store:KeyValueStore = [ : ]
                        
                        for ( key, dataValue ) in dataValues
                        {
                            store[ key ] = .init( data:dataValue, metadata:metadataValues[ key ] )
                        }
                        
                        self.getSubject( for:collection ).value = store
                    }
                }
                catch
                {
                    os_log( "disk restore error: %{public}@", log:self.log, type:.error, error.localizedDescription )
                }
            }
            
            self.restored = true
            os_log( "restore complete", log:self.log, type:.default )
            then?( )
        }
    }
    
    public func purge( )
    {
        diskQueue.async
        {
            os_log( "purging disk", log:self.log, type:.default )
            for collection in CollectionType.allCases
            {
                do
                {
                    try FileManager.default.removeItem( at:self.dataStorageURL( for:collection ) )
                }
                catch
                {
                    os_log( "disk purge error: %{public}@", log:self.log, type:.error, error.localizedDescription )
                }
                
                do
                {
                    try FileManager.default.removeItem( at:self.metadataStorageURL( for:collection ) )
                }
                catch
                {
                    os_log( "disk purge error: %{public}@", log:self.log, type:.error, error.localizedDescription )
                }
            }
        }
    }
}
