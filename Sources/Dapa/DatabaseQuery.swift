//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation

public struct DatabaseQuery{
    public var sql:DatabaseExpress
    public init(sql:DatabaseExpress){
        self.sql = sql
    }
    public func query<T:DatabaseResult>(db:Database,param:[String:Any] = [:]) throws ->[T] {
        let rs = try db.prepare(sql: self.sql.sqlCode)
        var results:[T] = []
        for i in param{
            try rs.bind(name: i.key, value: i.value)
        }
        while try rs.step() == .hasColumn {
            var result = T()
            rs.colume(model: &result)
            results.append(result)
        }
        rs.close()
        return results
    }
    public func exec(db:Database,param:[String:Any] = [:]) throws {
        let rs = try db.prepare(sql: self.sql.sqlCode)
        for i in param{
            try rs.bind(name: i.key, value: i.value)
        }
        try rs.step()
        rs.close()
    }
}

public protocol DatabaseQueryModel:DatabaseResult{
    static var queryDeclare:[DatabaseQueryColumeDeclare] { get }
    static var table:DatabaseGenerator.Select.JoinTable { get }
}

extension DatabaseQueryModel{
    public static func query(condition:DatabaseGenerator.DatabaseCondition? = nil,
                      groupBy:[String] = [],
                      orderBy: [DatabaseGenerator.OrderBy] = [],
                      limit:UInt64? = nil,
                      offset:UInt64? = nil)->DatabaseQuery{
        let array:[DatabaseGenerator.ResultColume] = self.queryDeclare.map { dqcd in
                .colume(name: dqcd.name)
        }
        let sql = DatabaseGenerator.Select(colume: array, tableName: self.table, condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)
        return DatabaseQuery(sql: sql)
    }
    public static func select(condition:DatabaseGenerator.DatabaseCondition? = nil,
                             groupBy:[String] = [],
                             orderBy: [DatabaseGenerator.OrderBy] = [],
                             limit:UInt64? = nil,
                             offset:UInt64? = nil)->DatabaseGenerator.Select{
        let array:[DatabaseGenerator.ResultColume] = self.queryDeclare.map { dqcd in
                .colume(name: dqcd.name)
        }
        return DatabaseGenerator.Select(colume: array, tableName: self.table, condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)
    }
    public static func createView(db:Database,
                            viewName:DatabaseGenerator.ItemName,
                            condition:DatabaseGenerator.DatabaseCondition? = nil,
                            groupBy:[String] = [],
                            orderBy: [DatabaseGenerator.OrderBy] = [],
                            limit:UInt64? = nil,
                            offset:UInt64? = nil){
        db.exec(sql: DatabaseGenerator.View(viewName: viewName, select: self.select(condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)).sqlCode)
        
    }
}


public protocol DatabaseViewModel:DatabaseQueryModel{
    static var view:DatabaseGenerator.ItemName { get }
    static var condition: Dapa.DatabaseGenerator.DatabaseCondition? { get }
    static var groupBy:[String] { get }
}

extension DatabaseViewModel{
    public static func View(db:Database,
                            orderBy: [DatabaseGenerator.OrderBy] = [],
                            limit:UInt64? = nil,
                            offset:UInt64? = nil){
        db.exec(sql: DatabaseGenerator.View(viewName: self.view, select: self.select(condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)).sqlCode)
        
    }
    public static func query(condition:DatabaseGenerator.DatabaseCondition? = nil,
                             groupBy:[String] = [],
                             orderBy: [DatabaseGenerator.OrderBy] = [],
                             limit:UInt64? = nil,
                             offset:UInt64? = nil)->DatabaseQuery{
        return DatabaseQuery(sql: DatabaseGenerator.Select(tableName: .init(table: self.view), condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset))
    }
}
