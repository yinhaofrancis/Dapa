//
//  DatabaseResult.swift
//  
//
//  Created by wenyang on 2023/3/15.
//

import Foundation


@dynamicMemberLookup
public protocol DatabaseResult{
    var model:Dictionary<String,Any> { get set}
    init()
}

extension DatabaseResult{
    public subscript(dynamicMember dynamicMember:String)->Any{
        get{
            self.model[dynamicMember] as Any
        }
        set{
            self.model[dynamicMember] = newValue
        }
    }
    public subscript<T>(dynamicMember dynamicMember:String)->T?{
        get{
            self.model[dynamicMember] as? T
        }
    }
}

public struct DatabaseResultModel:DatabaseResult{
    public init() {
        self.model = Dictionary()
    }
    public var model: Dictionary<String, Any>
}
