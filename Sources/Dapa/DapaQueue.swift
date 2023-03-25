//
//  DapaQueue.swift
//  
//
//  Created by wenyang on 2023/3/22.
//

import Foundation


final public class DapaNormalQueue:DapaQueue<DapaNormalConfig>{}

public class DapaQueue<DC:DapaConfig>{
    
    private var write:Dapa
    
    private var dbName:String
    
    private var semaphore:DispatchSemaphore = DispatchSemaphore(value: 3)

    private var queue:DispatchQueue = DispatchQueue(label: "DapaQueue", qos: .background, attributes: .concurrent, autoreleaseFrequency: .inherit, target: nil)
    
    public init(name:String) throws{
        let dap = try DC.write(name: name)
        self.write = dap
        self.dbName = name
    }
    
    public func transaction(_ callback:@escaping (Dapa) throws ->Void){
        self.queue.async(execute: DispatchWorkItem(flags: .barrier, block: {
            do{
                self.write.begin()
                try callback(self.write)
                self.write.commit()
            }catch{
                self.write.rollback()
            }
        }))
    }
    public func query(_ callback:@escaping (Dapa) throws ->Void){
        self.queue.async {
            self.semaphore.wait()
            defer{
                self.semaphore.signal()
            }
            do{
                let dp = try DC.read(name: self.dbName)
                defer{
                    dp.close()
                }
                try callback(dp)
            }catch{
                
            }
        }
    }
    deinit{
        self.write.close()
    }
}
