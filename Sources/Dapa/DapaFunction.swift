//
//  File.swift
//  
//
//  Created by wenyang on 2023/3/18.
//

import Foundation
import SQLite3


/// 方法参数值
public protocol DapaFunctionValue{}
extension Int:DapaFunctionValue{}
extension Int32:DapaFunctionValue{}
extension Int64:DapaFunctionValue{}
extension String:DapaFunctionValue{}
extension Double:DapaFunctionValue{}
extension Float:DapaFunctionValue{}
extension Data:DapaFunctionValue{}
extension Dapa{
    public struct Value{
        public let sqlValue:OpaquePointer
        
        public init(sqlValue:OpaquePointer){
            self.sqlValue = sqlValue
        }
        
        /// 读取参数
        /// - Returns: 参数值
        public func value<T:DapaFunctionValue>()->T{
            if T.self == Int32.self{
                return sqlite3_value_int(self.sqlValue) as! T
            }
            if T.self == Int64.self{
                return sqlite3_value_int64(self.sqlValue) as! T
            }
            if T.self == Int.self{
                if(MemoryLayout<Int>.size == 32){
                    return Int(sqlite3_value_int(self.sqlValue)) as! T
                }else{
                    return Int(sqlite3_value_int64(self.sqlValue)) as! T
                }
            }
            if T.self == Float.self{
                return Float(sqlite3_value_double(self.sqlValue)) as! T
            }
            if T.self == Double.self{
                return sqlite3_value_double(self.sqlValue) as! T
            }
            if T.self == String.self{
                guard let p = sqlite3_value_text(self.sqlValue) else { return "" as! T }
                let c = sqlite3_value_bytes(self.sqlValue)
                return String(data: Data(bytes: p, count: Int(c)), encoding: .utf8) as! T
            }
            if T.self == Data.self{
                guard let p = sqlite3_value_text(self.sqlValue) else { return "" as! T }
                let c = sqlite3_value_bytes(self.sqlValue)
                return Data(bytes: p, count: Int(c)) as! T
            }
            return 0 as! T
        }
    }
}


extension Dapa{
    /// 数据库方法
    public class Funtion{
        /// 创建数据库方法 实现 xFunc 并且 xStep xFinal 为 scalar 方法 不会统计数据
        ///  实现 xStep xFinal 并且 xFunc 为nil 为统计方法 类型 count() 方法
        /// - Parameters:
        ///   - name: 方法名
        ///   - nArg: 参数个数
        ///   - xFunc: scalar 方法
        ///   - xStep: aggregate 方法
        ///   - xStep: aggregate 结束方法
        public init(name: String,
                      nArg: Int32,
                      xFunc: Funtion.FunctionCallback? = nil,
                      xStep: Funtion.FunctionCallback? = nil,
                      xFinal: FinalCallback? = nil) {
            self.name = name
            self.nArg = nArg
            self.xFunc = xFunc
            self.xStep = xStep
            self.xFinal = xFinal
        }
        
        public typealias FunctionCallback = (Dapa.FunctionContext, [Dapa.Value])->Void
        public typealias FinalCallback = (Dapa.FunctionContext)->Void
        
        
        public let name:String
        public let nArg:Int32
        public var xFunc:FunctionCallback?
        public var xStep:FunctionCallback?
        public let xFinal:FinalCallback?
    }
}


extension Dapa{
    /// 方法上下文
    public struct FunctionContext{
        public let ctx:OpaquePointer
        public init(ctx: OpaquePointer) {
            self.ctx = ctx
        }
        /// 统计方法获取统计的数据空间
        /// - Returns: 数据指针 注意 swift 结构体数据修改后的变化
        public func pointer<T>()->UnsafeMutablePointer<T>?{
            let p = sqlite3_aggregate_context(self.ctx, Int32(MemoryLayout<T>.size))
            return p?.assumingMemoryBound(to: T.self)
        }
        /// 方法返回值
        /// - Parameter value: 返回数据 支持int32  int64 int Float double string data
        public func result<T>(value:T){
            if T.self == Int32.self{
                sqlite3_result_int(self.ctx, value as! Int32)
            }
            if T.self == Int64.self{
                sqlite3_result_int64(self.ctx, value as! Int64)
            }
            if T.self == Int.self{
                if(MemoryLayout<Int>.size == 32){
                    sqlite3_result_int(self.ctx, Int32(value as! Int))
                }else{
                    sqlite3_result_int64(self.ctx, sqlite3_int64(value as! Int))
                }
            }
            if T.self == Float.self{
                sqlite3_result_double(self.ctx, Double(value as! Float))
            }
            if T.self == Double.self{
                sqlite3_result_double(self.ctx, value as! Double)
            }
            if T.self == String.self{
                let str = value as! String
                let c = str.cString(using: .utf8)
                let p = UnsafeMutablePointer<CChar>.allocate(capacity: str.utf8.count)
                memcpy(p, c, str.utf8.count)
                sqlite3_result_text(self.ctx, p, Int32(str.utf8.count)) { p in
                    p?.deallocate()
                }
            }
            if T.self == Data.self{
                let str = value as! Data
                let m:UnsafeMutablePointer<UInt8> = UnsafeMutablePointer.allocate(capacity: str.count)
                str.copyBytes(to: m, count: str.count)
                sqlite3_result_blob(self.ctx, m, Int32(str.count)) { p in
                    p?.deallocate()
                }
            }
        }
    }
}
