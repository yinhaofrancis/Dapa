//
//  DatabaseBackup.swift
//  
//
//  Created by wenyang on 2023/3/18.
//

import Foundation
import SQLite3

public class DatabaseBackup{
    
    public var backupDb:Database
    
    public var database:Database
    
    public init(url:URL,database:Database) throws{
        self.backupDb = try Database(url: url)
        self.database = database
    }
    
    public init(name:String,database:Database) throws{
        self.backupDb = try Database(name: name)
        self.database = database
    }
    
    public func backup(){
        let back = sqlite3_backup_init(self.backupDb.sqlite!, "main", self.database.sqlite!, "main");
        if(back != nil){
            while(sqlite3_backup_remaining(back) > 0){
                sqlite3_backup_step(back, sqlite3_backup_pagecount(back))
            }
        }
        self.backupDb.close()
        self.database.close()
    }
}


