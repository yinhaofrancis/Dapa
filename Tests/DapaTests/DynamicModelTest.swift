//
//  DynamicModelTest.swift
//  
//
//  Created by wenyang on 2023/3/23.
//

import Foundation
import XCTest
import Dapa

final public class DynamicModelTest:XCTestCase{
    public var db:Dapa = try! Dapa(name: "db")
    
    public override func setUp() {
        super.setUp()
        TestModel.drop(db: self.db)
        TestModel.create(db: self.db)
        TestModelPrimaryKey.drop(db: self.db)
        TestModelPrimaryKey.create(db: self.db)
    }
    
    public func testCreate() throws {
        TestModel.drop(db: self.db)
        TestModel.create(db: self.db)
        XCTAssert(try TestModel.exist(db: self.db))
    }
    public func testInsert() throws {
        if(false == (try TestModel.exist(db: self.db))){
            TestModel.create(db: self.db)
        }
        var model = TestModel()
        model.testInt = 1;
        model.testFloat = 1.1;
        model.testString = "string"
        model.testData = "this is data".data(using: .utf8)
        model.testDate = Date()
        model.testJSON = ["json":"json"]
        try model.insert(db: self.db)
        
        var new = TestModel()
        new.rowid = 1;
        try new.sync(db: self.db)
        XCTAssert(new.testInt == 1)
        XCTAssert(new.testFloat == 1.1)
        XCTAssert(new.testString == "string")
        let data:Data? = new.testData
        XCTAssert(data != nil)
        
        
        var t = TestModelPrimaryKey()
        t.testInt = 1;
        t.testString = "tttt"
        try t.insert(db: self.db)
        do{
            try t.insert(db: self.db)
            XCTAssert(false)
        }catch{
            XCTAssert(true)
            t.testFloat = 0.01;
            try t.replace(db: self.db)
            var newv = TestModelPrimaryKey()
            newv.testInt = 1;
            try newv.sync(db: self.db)
            XCTAssert(newv.testFloat == 0.01)
            
            newv.testString = "this is testString"
            
            try newv.update(db: self.db)
            try t.sync(db: self.db)
            XCTAssert(t.testString == newv.testString)
            
            try newv.delete(db: self.db)
            
            try newv.sync(db: self.db)
            
            XCTAssert(newv.model.count == 0)
        }
        
        
    }
        
    func testError() throws{
        var t = TestModelPrimaryKey()
        t.testInt = 2;
        t.testString = "test null key";
        t.testError = 100;
        try t.insert(db: db)
        t = TestModelPrimaryKey()
        t.testInt = 2;
        try t.sync(db: self.db)
        XCTAssert(t.testError == nil)
        
        t.testInt = 13234
        try t.sync(db: self.db)
        XCTAssert(t.model.count == 0)
        
        var tt = TestModel()
        tt.rowid = "dasdadad"
        try tt.sync(db: self.db)
        XCTAssert(tt.model.count == 0)
        
        tt.testFloat = [1,2,3,4,5,6,7]
        tt.model.removeValue(forKey: "rowid")
        do{
            try tt.insert(db: db)
        }catch{
            print(error)
        }
        
        tt.rowid = 1;
        try tt.sync(db: self.db)
        print(tt)
    }
    deinit{
        self.db.close()
    }
}

final class testQuery:XCTestCase{
    public var db:Dapa = try! Dapa(name: "db")
    override func setUp() {
        super.setUp()
        TestModelPrimaryKey.drop(db: self.db)
        TestModelPrimaryKey.create(db: self.db)
        let a = (0 ..< 1000).map { i in
            var t = TestModelPrimaryKey()
            t.testInt = i
            t.testFloat = Float(i) + 0.1
            t.testString = "\(i) string"
            t.testData = "\(i) string".data(using: .utf8)
            t.testDate = Date(timeIntervalSince1970: TimeInterval(i))
            t.testJSON = ["\(i) key":"\(i) value"]
            return t
        }
        try! TestModelPrimaryKey.insert(type: .insert, models: a, db: self.db)
    }
    
    func testSelect() throws{
        let a:[TestModelPrimaryKey] = try TestModelPrimaryKey.select().query(db: self.db)
        XCTAssert(a.count == 1000)
        let condition = Dapa.Generator.Condition(stringLiteral: "testInt == 1").or(condition: "testInt > 99")
        let b:[TestModelPrimaryKey] = try TestModelPrimaryKey.select(condition:condition).query(db: self.db)
        XCTAssert(b.count == 901)
        
        let c:[TestModelPrimaryKey] = try TestModelPrimaryKey.select(condition:"testInt < 10",orderBy: [.init(colume: "testInt", asc: false)]).query(db: self.db)
        XCTAssert(c.count == 10)
        for i in 0 ..< 9{
            let l:Int = c[i].testInt!
            let r:Int = c[i + 1].testInt!
            XCTAssert(r < l)
        }
        
    }
    
    func testUpdate() throws{
        let v = "mkjh"
        try TestModelPrimaryKey.update(keyValues: ["testString":"@updated"],condition: "testInt < 1000").exec(db: self.db,param: ["@updated":v])
        let a:[TestModelPrimaryKey] = try TestModelPrimaryKey.select().query(db: self.db)
        for i in a{
            XCTAssert(i.testString == v)
        }
        let r: [Dapa.ResultModel] = try TestModelPrimaryKey.count().query(db: self.db)
        XCTAssert(r.first?.count == 1000)
    }
    func testInsert() throws{
 
        try TestModelPrimaryKey.delete(condition: "testInt < 100000").exec(db: self.db)
        let a = (0 ..< 1000).map { i in
            var t = TestModelPrimaryKey()
            t.testInt = i
            t.testFloat = Float(i) + 0.1
            t.testString = "\(i) string"
            t.testData = "\(i) string".data(using: .utf8)
            t.testDate = Date(timeIntervalSince1970: TimeInterval(i))
            t.testJSON = ["\(i) key":"\(i) value"]
            return t
        }
        try TestModelPrimaryKey.insert(type: .insert, models: a, db: self.db)
    }
    deinit{
        self.db.close()
    }
}

public struct TestModel:DapaModel{
    public static var tableName: String = "TestModel"
    
    public static var declare: [Dapa.ColumeDeclare]{
        [
            Dapa.ColumeDeclare(name: "testInt", type: .intDecType),
            Dapa.ColumeDeclare(name: "testFloat", type: .doubleDecType),
            Dapa.ColumeDeclare(name: "testString", type: .textDecType),
            Dapa.ColumeDeclare(name: "testData", type: .dataDecType),
            Dapa.ColumeDeclare(name: "testDate", type: .dateDecType),
            Dapa.ColumeDeclare(name: "testJSON", type: .jsonDecType)
            
        ]
    }
    
    public init() { }
    public var model: Dictionary<String, Any> = [:]
}

public struct TestModelPrimaryKey:DapaModel{
    public static var tableName: String = "TestModelPrimaryKey"
    
    public static var declare: [Dapa.ColumeDeclare]{
        [
            Dapa.ColumeDeclare(name: "testInt", type: .intDecType,primary: true),
            Dapa.ColumeDeclare(name: "testFloat", type: .doubleDecType),
            Dapa.ColumeDeclare(name: "testString", type: .textDecType,notNull: true),
            Dapa.ColumeDeclare(name: "testData", type: .dataDecType),
            Dapa.ColumeDeclare(name: "testDate", type: .dateDecType),
            Dapa.ColumeDeclare(name: "testJSON", type: .jsonDecType)
            
        ]
    }
    
    public init() { }
    public var model: Dictionary<String, Any> = [:]
}
