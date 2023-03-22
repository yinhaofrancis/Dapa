//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/8.
//

import Foundation


/// 查询表达式
public protocol DapaExpress{
    /// sql
    var sqlCode:String { get }
}
extension Dapa{
    /// sql生成器
    public class Generator {}
}

extension Dapa.Generator{
    
    /// 查询列的枚举
    public enum ResultColume:CustomStringConvertible{
        /// 列名
        /// - Parameter name: 列表达式
        case colume(name:String)
        /// 列名
        /// - Parameter name: 列表达式
        /// - Parameter alias: 别名
        case alias(name:String,alias:String)
        public var description: String{
            switch self{
                
            case .colume(name: let name):
                return name
            case .alias(name: let name, alias: let alias):
                return name + " AS " + alias
            }
        }
    }
    /// 排序
    public struct OrderBy:CustomStringConvertible{
        /// 创建排序参数
        /// - Parameters:
        ///   - colume: 排序的列
        ///   - asc: 是否正序
        public init(colume: String, asc: Bool) {
            self.colume = colume
            self.asc = asc
        }
        public var description: String{
            colume + " " + (asc ? "ASC" : "DESC")
        }
        public var colume:String
        public var asc:Bool
        
    }
    /// 查询表达式
    public struct Select:DapaExpress{
        /// 创建查询表达式
        /// - Parameters:
        ///   - colume: 列
        ///   - tableName: 表名 可以多表
        ///   - condition: 条件
        ///   - groupBy: 分组
        ///   - orderBy: 排序
        ///   - limit: 数据量
        ///   - offset: 偏移量
        public init(colume: [ResultColume] = [],
                   tableName: Dapa.Generator.Select.JoinTable,
                   condition: DatabaseCondition? = nil,
                   groupBy: [String] = [],
                   orderBy: [OrderBy] = [],
                   limit: UInt64? = nil,
                   offset: UInt64? = nil) {
            self.colume = colume
            self.tableName = tableName
            self.condition = condition
            self.groupBy = groupBy
            self.orderBy = orderBy
            self.limit = limit
            self.offset = offset
        }
        public var sqlCode: String{
            let c = colume.count > 0 ? colume.map{$0.description}.joined(separator: ",") : " * "
            let condition = self.condition != nil ? " WHERE " + self.condition!.sqlCode : ""
            let group = self.groupBy.count == 0 ? "" : " GROUP BY \(groupBy.joined(separator: ","))"
            let offset = offset == nil ? "" : " OFFSET " + offset!.description
            let limit = limit == nil ? "" : " LIMIT " + limit!.description
            let order = orderBy.count == 0 ? "" : " ORDER BY " + orderBy.map {$0.description}.joined(separator: ",")
            return "SELECT " + c + " from " + tableName.table + condition + group + order + limit + offset
        }
        
        public var colume:[ResultColume] = []
        public var tableName:JoinTable
        public var condition:DatabaseCondition? = nil
        public var groupBy:[String] = []
        public var orderBy:[OrderBy] = []
        public var limit:UInt64? = nil
        public var offset:UInt64? = nil
        
        /// 链接持续类型
        public enum TableJoin:String{
            case leftJoin = " LEFT JOIN "
            case crossJoin = " CROSS JOIN "
            case join = " JOIN "
            case innerJoin = " INNER JOIN "
        }
        /// 表名用查询 支持连表
        public struct JoinTable{
            public var table:String
            /// 创建表
            /// - Parameter table: 表名
            public init(table:ItemName){
                self.table = table.description
            }
            /// 链接另一个表
            /// - Parameters:
            ///   - type: 链接方式
            ///   - table: 表名对象
            /// - Returns: 表名
            public func join(type:TableJoin,table:ItemName)->JoinTable{
                var jt = JoinTable(table: .name(name: ""))
                jt.table = self.table + " " + type.rawValue + table.description
                return jt
            }
        }
    }
}

extension Dapa.Generator {
    /// 查询对象名，表，视图等
    public enum ItemName:CustomStringConvertible{
        case withSchema(schema:String,name:String)
        case name(name:String)
        public var description: String{
            switch self{
            case .withSchema(schema: let schema, name: let name):
                return schema + "." + name
            case .name(name: let name):
                return name
            }
        }
    }
    
    /// 创建表表达式
    public struct Table:DapaExpress{
        /// 创建表的表达式
        /// - Parameters:
        ///   - istemp: 是否是临时表
        ///   - ifNotExists: 如果不存在就创建
        ///   - tableName: 表名
        ///   - columeDefine: 列定义
        public init(istemp: Bool = false,
                    ifNotExists: Bool = false,
                    tableName: ItemName,
                    columeDefine: [Dapa.ColumeDeclare]) {
            self.istemp = istemp
            self.ifNotExists = ifNotExists
            self.tableName = tableName
            self.columeDefine = columeDefine
        }

        public var istemp:Bool = false
        
        public var ifNotExists:Bool = false
        
        public var tableName:ItemName
        
        public var columeDefine:[Dapa.ColumeDeclare]
        
        public var sqlCode:String{
            let code = "CREATE \( istemp ? "TEMP" : "") TABLE \(ifNotExists ? "IF NOT EXISTS" : "") \(tableName)"
            let cd = columeDefine
            let dec = cd.map{$0.name + " " + $0.type.rawValue + "\($0.notNull ? " NOT NULL" : "")"} .joined(separator: ",")
            let pd = cd.filter{$0.primary}.map{$0.name}.joined(separator: ",")
             
            let primary =  pd.count > 0 ? ",PRIMARY KEY (\(pd))" : ""
            
            let ud = cd.filter{$0.unique}.map{$0.name}.joined(separator: ",")
            let uniq = ud.count > 0 ? ",UNIQUE (\(ud))" : ""
            let def = "(\(dec)\(primary)\(uniq))"
            return code + def
        }
    }
}

extension Dapa.Generator {
    /// 插入表达式
    public struct Insert:DapaExpress{
        /// 插入类型
        public enum InsertType:String{
            // 直接插入 出错就报错
            case insert = "INSERT INTO"
            // 取代
            case replace = "REPLACE INTO"
            // 插入或取代
            case insertReplace = "INSERT OR REPLACE INTO"
        }
        public var sqlCode: String{
            "\(insertType.rawValue) \(table) \(colume.count == 0 ? "" : "(" + colume.joined(separator: ",") + ")")" +  value
        }
        
        public var table:String
        public var colume:[String]
        public var value:String
        public var insertType:InsertType
        
        /// 创建插入表达式
        /// - Parameters:
        ///   - insert: 插入类型
        ///   - table: 表名
        ///   - colume: 列名
        ///   - value: 值
        public init(insert:InsertType, table: ItemName, colume: [String], value: [String]) {
            self.table = table.description
            self.colume = colume
            self.insertType = insert
            self.value = " VALUES (\(value.joined(separator: ",")))"
        }
        /// 插入查询结果
        /// - Parameters:
        ///   - insert: 插入类型
        ///   - table: 表名
        ///   - colume: 列名
        ///   - value: 查询表达式
        public init(insert:InsertType,table: ItemName, colume: [String], value: Select) {
            self.insertType = insert
            self.table = table.description
            self.colume = colume
            self.value = value.sqlCode
        }
    }
}

extension Dapa.Generator {
    /// 删除表达式
    public struct Delete:DapaExpress{
        public var sqlCode: String{
            
            "DELETE FROM " + self.table.description + " WHERE " + condition.sqlCode
        }
        
        public var table:ItemName
        public var condition:DatabaseCondition
        
        /// 创建删除
        /// - Parameters:
        ///   - table: 表名
        ///   - condition: 条件
        public init(table: ItemName, condition:DatabaseCondition) {
            self.table = table
            self.condition = condition
        }
    }
}
extension Dapa.Generator {
    /// 更新表达式
    public struct Update:DapaExpress{
        /// 创建更新
        /// - Parameters:
        ///   - keyValue: 键值对
        ///   - table: 表名
        ///   - condition: 条件
        public init(keyValue: [String : String],
                    table: Dapa.Generator.ItemName,
                    condition: DatabaseCondition? = nil) {
            self.keyValue = keyValue
            self.table = table
            self.condition = condition
        }
        
        public var sqlCode: String{
            "UPDATE " + table.description + " SET " + keyValue.map{$0.key + "=" + $0.value}.joined(separator: ",") + (condition != nil ? " WHERE " + condition!.sqlCode : "")
        }
        
        public var keyValue:[String:String]
        public var table:ItemName
        public var condition:DatabaseCondition?
        
    }
}

extension Dapa.Generator {
    /// 创建索引表达式
    public struct Index:DapaExpress{
        /// 构造索引表达式
        /// - Parameters:
        ///   - isUnique: 是否唯一的
        ///   - ifNotExists: 不存在就创建
        ///   - indexName: 索引名
        ///   - tableName: 表名
        ///   - columes: 列名
        ///   - condition: 条件
        public init(isUnique: Bool = false,
                    ifNotExists: Bool = false,
                    indexName: ItemName,
                    tableName:String,
                    columes: [String],
                    condition:DatabaseCondition? = nil) {
            self.isUnique = isUnique
            self.ifNotExists = ifNotExists
            self.indexName = indexName
            self.tableName = tableName
            self.columes = columes
            self.condition = condition
        }

        public var isUnique:Bool = false
        
        public var ifNotExists:Bool = false
        
        public var tableName:String
        
        public var indexName:ItemName
        
        public var columes:[String]
        
        public var condition:DatabaseCondition?
        
        public var sqlCode:String{
            let code = "CREATE \( isUnique ? "UNIQUE" : "") INDEX \(ifNotExists ? "IF NOT EXISTS" : "") \(indexName) ON \(tableName) (\(columes.joined(separator: ",")))" + (condition == nil ? "" : " WHERE " + condition!.sqlCode)

            return code
        }
    }
}

extension Dapa.Generator{
    /// 创建视图
    public struct View:DapaExpress{
        public var sqlCode: String{
            return "CREATE \( istemp ? "TEMP" : "") VIEW \(ifNotExists ? "IF NOT EXISTS" : "") \(viewName) AS \(self.select.sqlCode)"
        }
        
        /// 构造视图
        /// - Parameters:
        ///   - istemp: 是否临时的
        ///   - ifNotExists: 不存在就创建
        ///   - viewName: 视图名
        ///   - select: 查询语句
        public init(istemp: Bool = false,
                    ifNotExists: Bool = true,
                    viewName: ItemName,
                    select:Select){
            self.istemp = istemp
            self.ifNotExists = ifNotExists
            self.viewName = viewName
            self.select = select
        }
        var istemp: Bool
        var ifNotExists: Bool
        var viewName: ItemName
        var select:Select
    }
}

extension Dapa.Generator{
    /// 查询条件
    public struct DatabaseCondition:DapaExpress,CustomStringConvertible,ExpressibleByStringLiteral{
        public typealias StringLiteralType = String
        
        public var sqlCode: String
        /// 字符串表达是构建
        /// - Parameter sqlCode: 字符串
        public init(stringLiteral sqlCode: String) {
            self.sqlCode = sqlCode
        }
        /// in
        /// - Parameter select: 查询语句
        /// - Returns: 条件
        public func `in`(select:Dapa.Generator.Select)->DatabaseCondition{
            
            return DatabaseCondition(stringLiteral: sqlCode + " in (\(select.sqlCode)) ")
        }
        /// and 语句
        /// - Parameter condition:  条件对象
        /// - Returns: 条件对象
        public func and(condition: DatabaseCondition)->DatabaseCondition{
            DatabaseCondition(stringLiteral: self.sqlCode + " and " + "(\(condition.sqlCode))")
        }
        /// or 语句
        /// - Parameter condition: 条件对象
        /// - Returns: 条件对象
        public func or(condition: DatabaseCondition)->DatabaseCondition{
            DatabaseCondition(stringLiteral: self.sqlCode + " or " + "(\(condition.sqlCode))")
        }
        public var description: String{
            return self.sqlCode
        }
    }

}

