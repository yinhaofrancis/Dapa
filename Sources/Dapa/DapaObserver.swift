//
//  DapaObserver.swift
//  
//
//  Created by wenyang on 2023/3/22.
//

import SQLite3
import SQLite3.Ext
import Foundation


extension Dapa{
    
    
    public enum ObserverEvent:Int32{
        case Update = 23
        case Delete = 9
        case Insert = 18
    }
    
    public class Observer{
       
        public typealias ObserverCallback = (ObserverEvent,String,String,Int64)->Void
        
        public var observerCallback:ObserverCallback
        public init(dapa:Dapa,callback:@escaping ObserverCallback){
            self.observerCallback = callback
            sqlite3_update_hook(dapa.sqlite, { observerptr, event, db, tb, rowid in
                guard let ev = ObserverEvent(rawValue: event) else { return }
                guard let ptr = observerptr else { return  }
                let umv = Unmanaged<Observer>.fromOpaque(ptr)
                let ob = umv.takeUnretainedValue()
                let dbstr = (db != nil) ? String(cString: db!): ""
                let tbstr = (tb != nil) ? String(cString: tb!): ""
                ob.observerCallback(ev,dbstr,tbstr,rowid)
            }, Unmanaged<Observer>.passUnretained(self).toOpaque())
        }
    }
}

