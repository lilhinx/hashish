import XCTest
import SwiftProtobuf
@testable import Hashish

extension ModelImportantDocument:Hashish.OptimisticLockable{ }

final class HashishTests:XCTestCase
{
    enum TableIdentifier:String, Collection, CustomStringConvertible
    {
        case foos
        case importantDocument
        
        var isOptimisticLockable:Bool
        {
            switch self
            {
            case .importantDocument:
                return true
            default:
                return false
            }
        }
        
        var valueType:Message.Type
        {
            switch self
            {
            case .foos:
                return ModelFoo.self
            case .importantDocument:
                return ModelImportantDocument.self
            }
        }
        
        var description: String
        {
            return rawValue
        }
        
        func decode( data:Data )->Message?
        {
            switch self
            {
            case .foos:
                return try? ModelFoo.init( serializedData:data )
            case .importantDocument:
                return try? ModelImportantDocument.init( serializedData:data )
            }
        }
        
        func decode( metadata:Data )->Message?
        {
            return nil
        }
        
    }
    
    func waitABit( )
    {
        usleep( 10000 )
    }

    func test_putData_removeData( )
    {
        let store:HashishTable<String,TableIdentifier> = .init( label:"test_putData_removeData", partition:"test" )
        
        let key:String = "bar"
        let num:Int32 = 42
        var foo:ModelFoo = .init( )
        foo.name = key
        foo.num = num
        
        let exp1 = expectation( description:"expected values" )
        
        let bar = store.publisher( for:.foos ).compactMap
        {
            ( table )->ModelFoo? in
            guard let model = table[ key ]?.data as? ModelFoo else
            {
                return nil
            }
            return model
        }
        
        let barObserver = bar.sink
        {
            ( bar ) in
            if bar.name == key && bar.num == num
            {
                exp1.fulfill( )
            }
        }
        
        
        store.readWrite( collection:.foos )
        {
            ( values, transaction ) in
            transaction.put( data:foo, for:key )
        }
        
        wait( for:[exp1], timeout:1 )
        XCTAssertNotNil( barObserver )
        barObserver.cancel( )
        
        
        
//        XCTAssertTrue( store.getAll( for:.foos ).keys.contains( key ) )
//
//
//        store.mutate( collection:.foos )
//        {
//            ( transaction ) in
//            transaction.remove( for:key )
//        }
//
//        waitABit( )
//
//        XCTAssertFalse( store.getAll( for:.foos ).keys.contains( key ) )
        
    }
    
    
    
//    func test_optimisticLocking( )
//    {
//        let key:String = "17b"
//        let store:HashishTable<TableIdentifier,String> = .init( label:"test_optimisticLocking" )
//
//        var document:ModelImportantDocument = .init( )
//        document.text = "Hello World"
//        document.version = 1
//
//
//        store.mutate( collection:.importantDocument )
//        {
//            ( transaction ) in
//            transaction.put( data:document, for:key )
//        }
//
//        waitABit( )
//
//        var foundDocument = store.getAll( for:.importantDocument )[ key ]?.data as? ModelImportantDocument
//        XCTAssertNotNil( foundDocument )
//        XCTAssertEqual( foundDocument!.text, "Hello World" )
//
//        document.text = "Hallo Warld"
//        document.version = 2
//
//        store.mutate( collection:.importantDocument )
//        {
//            ( transaction ) in
//            transaction.put( data:document, for:key )
//        }
//
//        waitABit( )
//
//        foundDocument = store.getAll( for:.importantDocument )[ key ]?.data as? ModelImportantDocument
//        XCTAssertNotNil( foundDocument )
//        XCTAssertEqual( foundDocument!.text, "Hallo Warld" )
//
//
//        document.text = "Hallo Warld (won't update because we don't increment version)"
//
//        store.mutate( collection:.importantDocument )
//        {
//            ( transaction ) in
//            transaction.put( data:document, for:key )
//        }
//
//        waitABit( )
//
//        foundDocument = store.getAll( for:.importantDocument )[ key ]?.data as? ModelImportantDocument
//        XCTAssertNotNil( foundDocument )
//        XCTAssertEqual( foundDocument!.text, "Hallo Warld" )
//
//    }

    static var allTests = [
        
        ( "test_putData_removeData", test_putData_removeData )
//        ( "test_metadata", test_metadata ),
//        ( "test_optimisticLocking", test_optimisticLocking )
    ]
}
