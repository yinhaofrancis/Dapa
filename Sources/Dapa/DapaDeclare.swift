//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation
import SQLite3
import SQLite3.Ext


extension Dapa{
    
    /// 列原始类型 既保存在数据库的类型
    public enum CollumnType{
        case nullCollumn
        case intCollumn
        case doubleCollumn
        case textCollumn
        case dataCollumn
    }

    /// 列定义类型
    public enum CollumnDecType:String{
        case intDecType = "INTEGER"
        case doubleDecType = "REAL"
        case textDecType = "TEXT"
        case dataDecType = "BLOB"
        case jsonDecType = "JSON"
        case dateDecType = "DATE"
        public var collumnType:CollumnType{
            switch(self){
            case .intDecType:
                return .intCollumn
            case .doubleDecType:
                return .doubleCollumn
            case .textDecType:
                return .textCollumn
            case .dataDecType:
                return .dataCollumn
            case .jsonDecType:
                return .textCollumn
            case .dateDecType:
                return .doubleCollumn
            }
        }
    }
    
    /// 列的定义 用于表的定义
    public class ColumeDeclare:QueryColumeDeclare{
        /// 表列申明
        /// - Parameters:
        ///   - name: 列名
        ///   - type: 定义类型
        ///   - primary: 主键
        ///   - unique: 值为一
        ///   - notNull: 不为空
        public init(name:String,
                    type: CollumnDecType,
                    primary: Bool = false,
                    unique: Bool = false,
                    notNull: Bool = false) {
            self.unique = unique
            self.notNull = notNull
            self.primary = primary
            super.init(name: name, colume: name, type: type)
        }
        
        public var unique:Bool
        public var notNull:Bool
        public var primary:Bool
    }
    
    /// 查询列的协议 用于声明查询结果的模型
    public class QueryColumeDeclare{
        /// 查询列名称
        public var name:String
        /// 结果的列名称
        public var colume:String
        /// 列定义类型
        public var type:CollumnDecType
        /// 查询列协议
        /// - Parameters:
        ///   - name:查询列名称
        ///   - colume: 结果的列名称 默认参数是空，动态属性的模型不用设置
        ///   - type: 查询列协议
        public init(name: String,colume:String? = nil,type: CollumnDecType) {
            self.name = name
            self.type = type
            self.colume = colume ?? name
        }
    }
}

/// 数据查询结果的模型协议 支持动态属性
@dynamicMemberLookup
public protocol DapaResult{
    /// 结果字典，动态属性的值
    var model:Dictionary<String,Any> { get set}
    
    init()
}

extension DapaResult{
    public subscript<T:Any>(dynamicMember dynamicMember:String)->T?{
        get{
            self.model[dynamicMember] as? T
        }
        set{
            self.model[dynamicMember] = newValue
        }
    }
    public subscript(dynamicMember dynamicMember:String)->Any{
        get{
            self.model[dynamicMember] as Any
        }
        set{
            self.model[dynamicMember] = newValue
        }
    }
}

extension Dapa{
    /// 简单结果模型，支持动态属性
    public struct ResultModel:DapaResult{
        public init() {
            self.model = Dictionary()
        }
        public var model: Dictionary<String, Any>
    }
}

/**
 
表的模型协议
 
 
 ### example
 
 创建简单表动态属性对象对象
 
 ```swift
 public struct Member:DapaModel{
     public static var tableName: String = "Member"
     
     public static var declare: [Dapa.ColumeDeclare] {
         [
             Dapa.ColumeDeclare(name: "domain", type: .textDecType,primary: true),
             Dapa.ColumeDeclare(name: "username", type: .textDecType),
             Dapa.ColumeDeclare(name: "remark", type: .textDecType),
             Dapa.ColumeDeclare(name: "avatar", type: .textDecType)
             
         ]
     }
     
     public init () {}
     
     public var model: Dictionary<String, Any> = [:]
     
 }

 public struct MemberOnline:DapaModel{
     public static var tableName: String = "MemberOnline"
     
     public static var declare: [Dapa.ColumeDeclare] {
         [
             Dapa.ColumeDeclare(name: "domain", type: .textDecType,primary: true),
             Dapa.ColumeDeclare(name: "online", type: .intDecType)
         ]
     }
     
     public init () {}
     
     public var model: Dictionary<String, Any> = [:]
     
 }

 public struct MemberRelation:DapaModel{
     public static var tableName: String = "MemberRelation"
     
     public static var declare: [Dapa.ColumeDeclare] {
         [
             Dapa.ColumeDeclare(name: "domain1", type: .textDecType,primary: true),
             Dapa.ColumeDeclare(name: "domain2", type: .textDecType,primary: true)
         ]
     }
     
     public init () {}
     
     public var model: Dictionary<String, Any> = [:]
     
 }
```
 */

public protocol DapaModel:DapaResult{
    /// 表名
    static var tableName:String { get }
    /// 列定义
    static var declare:[Dapa.ColumeDeclare] { get }
    
}
extension DapaModel{
    public static var hasPrimaryKey:Bool{
        self.declare.filter{$0.primary}.count > 0
    }
    public static var auto:Bool {
        return !self.hasPrimaryKey
    }
}
extension DapaModel{
    /// 生成查询对象
    /// - Parameters:
    ///   - condition: 条件
    ///   - orderBy: 排序
    ///   - limit: 数据量
    ///   - offset: 偏移量
    /// - Returns: 执行对象
    public static func select(condition:Dapa.Generator.Condition? = nil,
                              orderBy: [Dapa.Generator.OrderBy] = [],
                              limit:UInt64? = nil,
                              offset:UInt64? = nil)->Dapa.Query{
        let sql = Dapa.Generator.Select(tableName: .init(table: .name(name: self.tableName)),
                                        queryRowId: self.auto,
                                        condition: condition,
                                           orderBy: orderBy,
                                           limit: limit,
                                           offset: offset)
        return Dapa.Query(sql: sql)
    }
    /// 生成更新对象
    /// - Parameters:
    ///   - keyValues: 更新的列名和列值 列值支持数据参数 支持 @， ？等形式的参数
    ///   - condition: 条件
    /// - Returns: 执行对象
    public static func update(keyValues:[String:String],condition:Dapa.Generator.Condition? = nil)->Dapa.Query{
        let sql = Dapa.Generator.Update(keyValue: keyValues, table: .name(name: self.tableName), condition: condition)
        return Dapa.Query(sql: sql)
    }
    /// 生成删除对象
    /// - Parameter condition: 条件
    /// - Returns: 执行对象
    public static func delete(condition:Dapa.Generator.Condition)->Dapa.Query{
        let sql = Dapa.Generator.Delete(table: .name(name: self.tableName), condition: condition)
        return Dapa.Query(sql: sql)
    }
    /// 生成插入对象
    /// - Parameter type: 插入方式
    /// - Returns: 执行对象
    public static func insert(type:Dapa.Generator.Insert.InsertType,models:[Self],db:Dapa) throws{
        guard let keys = models.first?.model.keys else { return }
        let sql = Dapa.Generator.Insert(insert: type, table: .name(name: Self.tableName), colume: keys.map{$0}, value:keys.map{"@"+$0})
        let rs = try db.prepare(sql: sql.sqlCode)
        for i in models{
            rs.reset()
            try rs.bind(model: i)
            try rs.step()
        }
        rs.close()
    }
    /// 生成行数对象
    /// - Parameter condition: 条件
    /// - Returns: 执行对象
    public static func count(condition:Dapa.Generator.Condition? = nil)->Dapa.Query{
        let sql = Dapa.Generator.Select(colume: [.colume(name: "COUNT(*) as count")],tableName: .init(table: .name(name: Self.tableName)),condition: condition)
        return Dapa.Query(sql: sql)
    }
}
extension DapaModel{
    
    /// 创建表
    /// - Parameter db: 数据库
    public static func create(db:Dapa){
        let sql = Dapa.Generator.Table(ifNotExists: true, tableName: .name(name:self.tableName), columeDefine: self.declare)
        db.exec(sql: sql.sqlCode)
    }
    /// 创建索引
    /// - Parameters:
    ///   - index: 索引名称
    ///   - withColumn: 列名
    ///   - db: 数据库
    public static func createIndex(index:String,withColumn:String,db:Dapa){
        let sql = Dapa.Generator.Index(indexName: .name(name: index), tableName: Self.tableName, columes: [withColumn])
        db.exec(sql: sql.sqlCode)
    }
    /// sql 执行
    /// - Parameters:
    ///   - sql: 表达式
    ///   - db: 数据库
    public func DapaExpressExec(sql: DapaExpress, db: Dapa) throws {
        try Dapa.Query(sql: sql).exec(db: db) { rs in
            try rs.bind(model: self)
            try rs.step()
        }
    }
    
    /// 模型插入
    /// - Parameter db: 数据库
    public func insert(db:Dapa) throws {
        let col = Self.declare.map{$0.name}
        let sql = Dapa.Generator.Insert(insert: .insert, table: .name(name: Self.tableName), colume:col , value: col.map { "@" + $0 })
        try DapaExpressExec(sql: sql, db: db)
    }
    /// 取代模型
    /// - Parameter db: 数据
    public func replace(db:Dapa) throws {
        let col = Self.declare.map{$0.name}
        let sql = Dapa.Generator.Insert(insert: .insertReplace, table: .name(name: Self.tableName), colume:col , value: col.map { "@" + $0 })
        try self.DapaExpressExec(sql: sql, db: db)
    }
    
    /// 更新模型 需要主键
    /// - Parameter db: 数据库
    public func update(db:Dapa) throws {
        
        let kv:[String:String] = self.model.keys.reduce(into: [:]) { partialResult, s in
            partialResult[s] = "@" + s
        }
        let sql = Dapa.Generator.Update(keyValue: kv, table: .name(name: Self.tableName), condition: Dapa.Generator.Condition(stringLiteral: self.primaryCondition))
                                           
        try self.DapaExpressExec(sql: sql, db: db)
    }
    /// 删除 需要主键
    /// - Parameter db: 数据库
    public func delete(db:Dapa) throws {
 
        let sql = Dapa.Generator.Delete(table: .name(name: Self.tableName), condition: Dapa.Generator.Condition(stringLiteral: self.primaryCondition))
        try self.DapaExpressExec(sql: sql, db: db)
    }
    /// 同步 查找数据并更新对象 需要主键
    /// - Parameter db: 数据库
    public mutating func sync(db:Dapa) throws {
        let sql = Dapa.Generator.Select(
            tableName: .init(table: .name(name: Self.tableName)),
            queryRowId: Self.auto,
            condition: Dapa.Generator.Condition(stringLiteral: self.primaryCondition))
        let rs = try db.prepare(sql: sql.sqlCode)
        try rs.bind(model: self)
        try rs.step()
        rs.colume(model: &self)
        rs.close()
    }
    
    /// 删除表
    /// - Parameter db: 数据库
    public static func drop(db:Dapa){
        db.exec(sql: "DROP TABLE \(Self.tableName)")
    }
    
    
    /// 表存在
    /// - Parameter db: 数据库
    /// - Returns: 存在状态
    public static func exist(db:Dapa) throws ->Bool{
        return try db.tableExist(name: self.tableName)
    }
    public static func queryById(rowid:Int64,db:Dapa)throws->Self?{
        let jt = Dapa.Generator.Select.JoinTable(table: .name(name: self.tableName))
        let sql = Dapa.Generator.Select(tableName: jt,condition: "rowid = @rd")
        let out:Self? = try Dapa.Query(sql: sql).query(db: db,param: ["@rd":rowid]).first
        return out
    }
    /// 主键where 条件
    public var primaryCondition:String{
        if(Self.auto){
            return "rowid=@rowid"
        }else{
            return Self.declare.filter { $0.primary }.map{ $0.name + "=@" + $0.name }.joined(separator: " and ")
        }
    }
}
