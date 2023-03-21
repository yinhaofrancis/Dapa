//
//  DatabaseWrapModel.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation

public class DatabaseWrapColume:DatabaseColumeDeclare{
    public var value: Any

    public init(value: Any, name: String,
                type: CollumnDecType,
                primary: Bool = false,
                unique: Bool = false,
                notNull: Bool = false) {
        self.value = value
        super.init(name: name, type: type, primary: primary, unique: unique, notNull: notNull)
    }
}
@propertyWrapper
public class DapaColume<T>:DatabaseWrapColume{
    public var wrappedValue: T{
        get{
            self.value as! T
        }
        set{
            self.value = newValue
        }
    }
    public init(wrappedValue: T, type: CollumnDecType, primary: Bool = false, unique: Bool = false, notNull: Bool = false) {
        super.init(value: wrappedValue, name: "", type: type, primary: primary, unique: unique, notNull: notNull)
    }
}

public protocol DatabaseWrapModel:DatabaseModel{

}
extension DatabaseWrapModel{
    public static var declare: [DatabaseColumeDeclare]{
        Mirror(reflecting: Self()).children.filter { v in
            v.value is DatabaseColumeDeclare
        }.map { v in
            let s = v.value as! DatabaseWrapColume
            let l = v.label!
            s.name = String(l[l.index(after: l.startIndex)..<l.endIndex])
            return s
        }
    }
    public var model: Dictionary<String, Any>{
        get{
            Mirror(reflecting: self).children.filter { v in
                v.value is DatabaseColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! DatabaseWrapColume).value)
            }.reduce(into: [:]) { partialResult, kv in
                if(kv.0!.hasPrefix("_")){
                    let v = kv.0!
                    partialResult[String(v[v.index(after: v.startIndex)..<v.endIndex])] = kv.1
                }
            }
        }
        set{
            let kv:[String:DatabaseWrapColume] = Mirror(reflecting: self).children.filter { v in
                v.value is DatabaseColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! DatabaseWrapColume))
            }.reduce(into: [:]) { partialResult, kv in
                if(kv.0!.hasPrefix("_")){
                    let v = kv.0!
                    partialResult[String(v[v.index(after: v.startIndex)..<v.endIndex])] = kv.1
                }
            }
            
            newValue.forEach { k in
                kv[k.key]?.value = k.value
            }
        }
        
    }
}
