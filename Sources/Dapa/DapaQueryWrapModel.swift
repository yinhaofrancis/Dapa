//
//  DapaWrapModel.swift
//
//
//  Created by wenyang on 2023/3/15.
//

import Foundation
extension Dapa{
    public class QueryWrapColume:Dapa.QueryColumeDeclare{
        public var value: Any

        public init(value: Any,
                    name: String,
                    colume:String,
                    type: CollumnDecType) {
            self.value = value
            super.init(name: name, colume: colume, type: type)
        }
    }
    
    /// 查询对象属性
    @propertyWrapper
    public class QueryColume<T>:Dapa.QueryWrapColume{
        public var wrappedValue: T{
            get{
                self.value as! T
            }
            set{
                self.value = newValue
            }
        }
        /// l
        /// - Parameters:
        ///   - wrappedValue: wrap 的属性
        ///   - name: 查询的列名
        ///   - colume: 结果的列名
        ///   - type: 类型
        public init(wrappedValue: T, name:String,colume:String? = nil,type: CollumnDecType) {
            super.init(value: wrappedValue, name: name, colume: colume ?? name, type: type)
        }
    }
}


/**
 静态查询对象协议
 
 
 ## example
 
```swift
 public struct MemberStaticDisplay:DapaQueryWrapModel{
     
     public static var table: Dapa.Generator.Select.JoinTable{
         .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline")).join(type: .join, table: .name(name: "MemberRelation"))
     }
     
     public init(){ }
     
     @Dapa.QueryColume(name:"MemberOnline.domain",colume: "domain",type: .textDecType)
     public var domain:String = ""
     @Dapa.QueryColume(name: "username",type: .textDecType)
     public var username:String = ""
     @Dapa.QueryColume(name: "remark",type: .textDecType)
     public var remark:String = ""
     @Dapa.QueryColume(name: "avatar",type: .textDecType)
     public var avatar:String = ""
     @Dapa.QueryColume(name: "online",type: .textDecType)
     public var online:String = ""

 }
 ```
 
 */
public protocol DapaQueryWrapModel:DapaQueryModel{

}
extension DapaQueryWrapModel{
    public static var queryDeclare: [Dapa.QueryColumeDeclare]{
        Mirror(reflecting: Self()).children.filter { v in
            v.value is Dapa.QueryColumeDeclare
        }.map { v in
            let s = v.value as! Dapa.QueryColumeDeclare
            return s
        }
    }
    public var model: Dictionary<String, Any>{
        get{
            Mirror(reflecting: self).children.filter { v in
                v.value is Dapa.QueryColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! Dapa.QueryWrapColume))
            }.reduce(into: [:]) { partialResult, kv in
                partialResult[kv.1.colume] = kv.1.value
            }
        }
        set{
            let kv:[String:Dapa.QueryWrapColume] = Mirror(reflecting: self).children.filter { v in
                v.value is Dapa.QueryColumeDeclare && v.label != nil
            }.map { v in
                (v.label,(v.value as! Dapa.QueryWrapColume))
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
