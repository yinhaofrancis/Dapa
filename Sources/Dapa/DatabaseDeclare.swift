//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation
import SQLite3
import SQLite3.Ext

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

public class DatabaseColumeDeclare:DatabaseQueryColumeDeclare{
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
    
    public var unique:Bool = false
    public var notNull:Bool = false
    public var primary:Bool = false
}

public class DatabaseQueryColumeDeclare{
    public var name:String
    public var colume:String
    public var type:CollumnDecType
    public init(name: String,colume:String? = nil,type: CollumnDecType) {
        self.name = name
        self.type = type
        self.colume = colume ?? name
    }
}
