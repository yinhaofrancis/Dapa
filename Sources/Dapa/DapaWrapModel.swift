//
//  DapaWrapModel.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation

extension Dapa{
    public class WrapColume:Dapa.ColumeDeclare{
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
    /// 表对象的属性
    @propertyWrapper
    public class DapaColume<T>:Dapa.WrapColume{
        public var wrappedValue: T{
            get{
                self.value as! T
            }
            set{
                self.value = newValue
            }
        }
        /// 创建表对象
        /// - Parameters:
        ///   - wrappedValue: wrap 属性
        ///   - type: 类型
        ///   - primary: 主键
        ///   - unique: 唯一
        ///   - notNull: 非空
        public init(wrappedValue: T, type: CollumnDecType, primary: Bool = false, unique: Bool = false, notNull: Bool = false) {
            super.init(value: wrappedValue, name: "", type: type, primary: primary, unique: unique, notNull: notNull)
        }
    }
    @propertyWrapper
    public class DapaRowId:Dapa.WrapColume{
        public var wrappedValue: Int64{
            get{
                self.value as! Int64
            }
            set{
                self.value = newValue
            }
        }
        public init(wrappedValue: Int64){
            super.init(value: wrappedValue, name: "rowid", type: .intDecType)
        }
    }
}



/**
 静态数据模型协议
 
 
 ## example
 ```swift
 public struct MemberStatic:DapaWrapModel{
     public static var tableName: String{
         "MemberStatic"
     }
     
     public init() {}
     
     @Dapa.DapaColume(type: .textDecType,primary: true)
     public var domain:String = ""
     @Dapa.DapaColume(type:.textDecType)
     public var username:String = ""
     @Dapa.DapaColume(type: .textDecType)
     public var remark:String = ""
     @Dapa.DapaColume(type: .textDecType)
     public var avatar:String = ""
     
 }
 ```
 
 */
public protocol DapaWrapModel:DapaModel{

}

extension DapaWrapModel{
    public static var declare: [Dapa.ColumeDeclare]{
        Mirror(reflecting: Self()).children.filter { v in
            v.value is Dapa.ColumeDeclare && (v.value as! Dapa.ColumeDeclare).name != "rowid"
        }.map { v in
            let s = v.value as! Dapa.WrapColume
            let l = v.label!
            s.name = String(l[l.index(after: l.startIndex)..<l.endIndex])
            return s
        }
    }
    public var model: Dictionary<String, Any>{
        get{
            Mirror(reflecting: self).children.filter { v in
                v.value is Dapa.ColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! Dapa.WrapColume).value)
            }.reduce(into: [:]) { partialResult, kv in
                if(kv.0!.hasPrefix("_")){
                    let v = kv.0!
                    partialResult[String(v[v.index(after: v.startIndex)..<v.endIndex])] = kv.1
                }
            }
        }
        set{
            let kv:[String:Dapa.WrapColume] = Mirror(reflecting: self).children.filter { v in
                v.value is Dapa.ColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! Dapa.WrapColume))
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
