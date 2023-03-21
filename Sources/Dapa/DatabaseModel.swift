//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/8.
//
import Foundation
import SQLite3
import SQLite3.Ext

public protocol DatabaseModel:DatabaseResult{
    static var tableName:String { get }
    static var declare:[DatabaseColumeDeclare] { get }
    
}

extension DatabaseModel{
    public static func select(condition:DatabaseGenerator.DatabaseCondition? = nil,
                              orderBy: [DatabaseGenerator.OrderBy] = [],
                              limit:UInt64? = nil,
                              offset:UInt64? = nil)->DatabaseQuery{
        let sql = DatabaseGenerator.Select(tableName: .init(table: .name(name: self.tableName)),
                                           condition: condition,
                                           orderBy: orderBy,
                                           limit: limit,
                                           offset: offset)
        return DatabaseQuery(sql: sql)
    }
    public static func update(keyValues:[String:String],condition:DatabaseGenerator.DatabaseCondition? = nil)->DatabaseQuery{
        let sql = DatabaseGenerator.Update(keyValue: keyValues, table: .name(name: self.tableName), condition: condition)
        return DatabaseQuery(sql: sql)
    }
    public static func delete(condition:DatabaseGenerator.DatabaseCondition)->DatabaseQuery{
        let sql = DatabaseGenerator.Delete(table: .name(name: self.tableName), condition: condition)
        return DatabaseQuery(sql: sql)
    }
    public func insert(type:DatabaseGenerator.Insert.InsertType)->DatabaseQuery{
        let keys = self.model.keys
        let sql = DatabaseGenerator.Insert(insert: type, table: .name(name: Self.tableName), colume: keys.map{$0}, value:keys.map{"@"+$0})
        return DatabaseQuery(sql: sql)
    }
    public static func count(condition:DatabaseGenerator.DatabaseCondition? = nil)->DatabaseQuery{
        let sql = DatabaseGenerator.Select(colume: [.colume(name: "COUNT(*) as count")],tableName: .init(table: .name(name: Self.tableName)),condition: condition)
        return DatabaseQuery(sql: sql)
    }
}
extension DatabaseModel{

    public static func create(db:Database){
        let sql = DatabaseGenerator.Table(ifNotExists: true, tableName: .name(name:self.tableName), columeDefine: self.declare)
        db.exec(sql: sql.sqlCode)
    }
    public static func createIndex(index:String,withColumn:String,db:Database){
        let sql = DatabaseGenerator.Index(indexName: .name(name: index), tableName: Self.tableName, columes: [withColumn])
        db.exec(sql: sql.sqlCode)
    }
    public func insert(db:Database) throws {
        let col = Self.declare.map{$0.name}
        let sql = DatabaseGenerator.Insert(insert: .insert, table: .name(name: Self.tableName), colume:col , value: col.map { "@" + $0 })
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        _ = try rs.step()
        rs.close()
    }
    public func replace(db:Database) throws {
        let col = Self.declare.map{$0.name}
        let sql = DatabaseGenerator.Insert(insert: .insertReplace, table: .name(name: Self.tableName), colume:col , value: col.map { "@" + $0 })
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        try rs.step()
        rs.close()
    }
    
    public func update(db:Database) throws {
        
        let kv:[String:String] = self.model.keys.reduce(into: [:]) { partialResult, s in
            partialResult[s] = "@" + s
        }
        let sql = DatabaseGenerator.Update(keyValue: kv, table: .name(name: Self.tableName), condition: DatabaseGenerator.DatabaseCondition(stringLiteral: self.primaryCondition))
                                           
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        try rs.step()
        rs.close()
    }
    public func delete(db:Database) throws {
 
        let sql = DatabaseGenerator.Delete(table: .name(name: Self.tableName), condition: DatabaseGenerator.DatabaseCondition(stringLiteral: self.primaryCondition))
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        _ = try rs.step()
        rs.close()
    }
    public mutating func sync(db:Database) throws {
        let sql = DatabaseGenerator.Select(tableName: .init(table: .name(name: Self.tableName)),condition: DatabaseGenerator.DatabaseCondition(stringLiteral: self.primaryCondition))
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        try rs.step()
        rs.colume(model: &self)
        rs.close()
    }
    public var primaryCondition:String{
        Self.declare.filter { $0.primary }.map{ $0.name + "=@" + $0.name }.joined(separator: " and ")
    }
}
