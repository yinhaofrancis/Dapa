//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation

extension Dapa{
    /// 执行查询的对象
    public struct Query{
        public var sql:DapaExpress
        public init(sql:DapaExpress){
            self.sql = sql
        }
        /// 查询
        /// - Parameters:
        ///   - db: 数据库
        ///   - param: 参数  数据可以是@参数
        /// - Returns: 结果集合
        public func query<T:DapaResult>(db:Dapa,param:[String:Any] = [:]) throws ->[T] {
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
        /// 执行
        /// - Parameters:
        ///   - db: 数据库
        ///   - param: 参数 数据可以是@参数
        public func exec(db:Dapa,param:[String:Any] = [:]) throws {
            let rs = try db.prepare(sql: self.sql.sqlCode)
            for i in param{
                try rs.bind(name: i.key, value: i.value)
            }
            try rs.step()
            rs.close()
        }
    }
}


/**
 查询对象
 
 创建查询对象
 ```swift
 public struct MemberCanVisibleQuery:DapaQueryModel{
     
     public init() {
         self.model = [:]
     }
     
     public static var queryDeclare: [Dapa.QueryColumeDeclare] = [
         .init(name: "MemberOnline.domain", type: .textDecType),
         .init(name: "username", type: .textDecType),
         .init(name: "remark", type: .textDecType),
         .init(name: "avatar", type: .textDecType),
         .init(name: "online", type: .textDecType),
     ]
     
     public static var table: Dapa.Generator.Select.JoinTable{
         .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline"))
     }
     
     public var model: Dictionary<String, Any>
     
 }
```
 */
public protocol DapaQueryModel:DapaResult{
    /// 查询定义
    static var queryDeclare:[Dapa.QueryColumeDeclare] { get }
    /// 查询的表名 可以链接查询
    static var table:Dapa.Generator.Select.JoinTable { get }
}

extension DapaQueryModel{
    /// 执行查询
    /// - Parameters:
    ///   - condition: 条件
    ///   - groupBy: 分组
    ///   - orderBy: 排序
    ///   - limit: 数据量
    ///   - offset: 偏移量
    /// - Returns: 结果
    public static func query(condition:Dapa.Generator.Condition? = nil,
                             groupBy:[String] = [],
                             orderBy: [Dapa.Generator.OrderBy] = [],
                             limit:UInt64? = nil,
                             offset:UInt64? = nil)->Dapa.Query{
        let array:[Dapa.Generator.ResultColume] = self.queryDeclare.map { dqcd in
                .colume(name: dqcd.name)
        }
        let sql = Dapa.Generator.Select(colume: array, tableName: self.table, condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)
        return Dapa.Query(sql: sql)
    }
    /// 生成查询语句
    /// - Parameters:
    ///   - condition: 条件
    ///   - groupBy: 分组
    ///   - orderBy: 排序
    ///   - limit: 数据量
    ///   - offset: 偏移量
    /// - Returns: 查询语句
    public static func select(condition:Dapa.Generator.Condition? = nil,
                             groupBy:[String] = [],
                             orderBy: [Dapa.Generator.OrderBy] = [],
                             limit:UInt64? = nil,
                             offset:UInt64? = nil)->Dapa.Generator.Select{
        let array:[Dapa.Generator.ResultColume] = self.queryDeclare.map { dqcd in
                .colume(name: dqcd.name)
        }
        return Dapa.Generator.Select(colume: array, tableName: self.table, condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)
    }
    /// 用查询创建视图
    /// - Parameters:
    ///   - db: 数据库
    ///   - viewName: 视图名
    ///   - condition: 条件
    ///   - groupBy: 分组
    ///   - orderBy: 排序
    ///   - limit: 数据量
    ///   - offset: 偏移量
    public static func createView(db:Dapa,
                            viewName:Dapa.Generator.ItemName,
                            condition:Dapa.Generator.Condition? = nil,
                            groupBy:[String] = [],
                            orderBy: [Dapa.Generator.OrderBy] = [],
                            limit:UInt64? = nil,
                            offset:UInt64? = nil){
        db.exec(sql: Dapa.Generator.View(viewName: viewName, select: self.select(condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)).sqlCode)
        
    }
}

/**
 视图对象
 ## example
 
 
 ```swift
 
 public struct MemberCanVisible:DapaViewModel{
     public static var groupBy: [String] = []
     
     
     public static var view: Dapa.Generator.ItemName {
         Dapa.Generator.ItemName.name(name: "MemberCanVisible")
     }
     
     public static var condition: Dapa.Generator.Condition?{
         
         return Dapa.Generator.Condition(stringLiteral: "MemberOnline.domain = Member.domain").and(condition: "MemberOnline.online = 1")
     }
     
     public init() {
         self.model = [:]
     }
     
     public static var queryDeclare: [Dapa.QueryColumeDeclare] = [
         .init(name: "MemberOnline.domain", type: .textDecType),
         .init(name: "username", type: .textDecType),
         .init(name: "remark", type: .textDecType),
         .init(name: "avatar", type: .textDecType),
         .init(name: "online", type: .textDecType),
     ]
     
     public static var table: Dapa.Generator.Select.JoinTable{
         .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline"))
     }
     
     public var model: Dictionary<String, Any>
     
 }

 
 ```
 
 */
public protocol DapaViewModel:DapaQueryModel{
    /// 视图名
    static var view:Dapa.Generator.ItemName { get }
    /// 条件
    static var condition: Dapa.Generator.Condition? { get }
    /// 分组
    static var groupBy:[String] { get }
}

extension DapaViewModel{
    /// 创建视图
    /// - Parameters:
    ///   - db: 数据库
    ///   - orderBy: 排序
    ///   - limit: 数量
    ///   - offset: 偏移量
    public static func View(db:Dapa,
                            orderBy: [Dapa.Generator.OrderBy] = [],
                            limit:UInt64? = nil,
                            offset:UInt64? = nil){
        db.exec(sql: Dapa.Generator.View(viewName: self.view, select: self.select(condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset)).sqlCode)
        
    }
    /// 创建查询
    /// - Parameters:
    ///   - condition: 条件
    ///   - groupBy: 分组
    ///   - orderBy: 排序
    ///   - limit: 数据量
    ///   - offset: 偏移量
    /// - Returns: 查询对象
    public static func query(condition:Dapa.Generator.Condition? = nil,
                             groupBy:[String] = [],
                             orderBy: [Dapa.Generator.OrderBy] = [],
                             limit:UInt64? = nil,
                             offset:UInt64? = nil)->Dapa.Query{
        return Dapa.Query(sql: Dapa.Generator.Select(tableName: .init(table: self.view), condition: condition, groupBy: groupBy, orderBy: orderBy, limit: limit, offset: offset))
    }
}
