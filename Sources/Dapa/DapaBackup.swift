//
//  DatabaseBackup.swift
//  
//
//  Created by wenyang on 2023/3/18.
//

import Foundation
import SQLite3
extension Dapa{
    /// 数据库备份
    public class Backup{
        
        public var backupDb:Dapa
        
        public var database:Dapa
        
        public init(url:URL,database:Dapa) throws{
            self.backupDb = try Dapa(url: url)
            self.database = database
        }
        
        public init(name:String,database:Dapa) throws{
            self.backupDb = try Dapa(name: name)
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
}



