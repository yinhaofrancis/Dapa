//
//  DatabaseWrapModel.swift
//
//
//  Created by wenyang on 2023/3/15.
//

import Foundation

public class DatabaseQueryWrapColume:DatabaseQueryColumeDeclare{
    public var value: Any

    public init(value: Any,
                name: String,
                colume:String,
                type: CollumnDecType) {
        self.value = value
        super.init(name: name, colume: colume, type: type)
    }
}
@propertyWrapper
public class DapaQueryColume<T>:DatabaseQueryWrapColume{
    public var wrappedValue: T{
        get{
            self.value as! T
        }
        set{
            self.value = newValue
        }
    }
    public init(wrappedValue: T, name:String,colume:String? = nil,type: CollumnDecType) {
        super.init(value: wrappedValue, name: name, colume: colume ?? name, type: type)
    }
}

public protocol DatabaseQueryWrapModel:DatabaseQueryModel{

}
extension DatabaseQueryWrapModel{
    public static var queryDeclare: [DatabaseQueryColumeDeclare]{
        Mirror(reflecting: Self()).children.filter { v in
            v.value is DatabaseQueryColumeDeclare
        }.map { v in
            let s = v.value as! DatabaseQueryColumeDeclare
            return s
        }
    }
    public var model: Dictionary<String, Any>{
        get{
            Mirror(reflecting: self).children.filter { v in
                v.value is DatabaseQueryColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! DatabaseQueryWrapColume))
            }.reduce(into: [:]) { partialResult, kv in
                partialResult[kv.1.colume] = kv.1.value
            }
        }
        set{
            let kv:[String:DatabaseQueryWrapColume] = Mirror(reflecting: self).children.filter { v in
                v.value is DatabaseQueryColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! DatabaseQueryWrapColume))
            }.reduce(into: [:]) { partialResult, kv in
                if(kv.0!.hasPrefix("_")){
                    partialResult[kv.1.colume] = kv.1
                }
            }
            
            newValue.forEach { k in
                kv[k.key]?.value = k.value
            }
        }
        
    }
}
