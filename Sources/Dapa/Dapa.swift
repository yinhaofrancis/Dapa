//
//  Database.swift
//  Alpha
//
//  Created by hao yin on 2022/4/12.
//

import Foundation
import SQLite3
import SQLite3.Ext

/**
数据库对象 sqlite3 封装
## example
 ```swift
 func testTable () throws{
     let db = try Dapa(name: "db") //创建数据库

     MemberStatic.create(db: db) // 创建表
 
     let a = MemberStatic()
     a.avatar = "http://xxxx"
     a.domain = "1"
     a.remark = "remark"
     a.username = "username 1"
 
     try a.insert(db: db) // 插入数据
 
     var b:MemberStatic = MemberStatic()
     b.domain = "1"
 
     try b.sync(db: db) // 获取数据
 
     db.close()
 }
 
 func testQuery() throws{
    let db = try Dapa(name: "db") //创建数据库
 
    // 创建方法
    // func go(a){ return a + 1 }
    db.registerFunction(function: Dapa.Funtion(name: "go", nArg: 1,xFunc: { ctx, params in
        let param:Int = params.first!.value()
        ctx.result(value: param + 1)
    
    }))
 
 
    let condition = Dapa.Generator.Condition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = @user2") .and(condition: "Member.domain = MemberRelation.domain1")
    let user:[MemberStaticDisplay] = try MemberStaticDisplay.query(condition: condition,groupBy: ["domain1"]).query(db: db,param: ["@user2":10])
    // 执行的sql SELECT MemberOnline.domain,username,remark,avatar,online from Member  JOIN MemberOnline  JOIN MemberRelation WHERE MemberOnline.domain == Member.domain and (MemberRelation.domain2 = @user2) and (Member.domain = MemberRelation.domain1) GROUP BY domain1
    //user2 = 10
 
 
    let st:[MemberDisplay] = try MemberDisplay.query(condition: "domain2=go(@test)").query(db: db,param: ["@test":"88"])
    // SELECT  *  from MemberDisplay WHERE domain2=go(@test)
    // test = 88
 }
 ```
 */
public struct Dapa:Hashable{
    
    // MARK: store property
    /// 数据库地址
    public let url:URL
    
    /// sqlite3 指针
    public private(set) var sqlite:OpaquePointer?
    
    // MARK: init
    /// 数据库创建
    /// - Parameters:
    ///   - url: 数据库URL
    ///   - readonly: 是否只读
    ///   - mutex: 是否互斥
    public init(url:URL,readonly:Bool = false,mutex:Bool = false) throws {
        self.url = url
        #if DEBUG
        print(url)
        #endif
        let r = readonly ? SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX  : (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | (mutex ? SQLITE_OPEN_FULLMUTEX: SQLITE_OPEN_NOMUTEX))
        if sqlite3_open_v2(url.path, &self.sqlite, r , nil) != noErr || self.sqlite == nil{
            throw NSError(domain: "数据库打开失败", code: 0)
        }
    }
    /// 创建内存数据库
    public init() throws {
        self.url = URL(string: "file::memory:?cache=shared")!
        if sqlite3_open(self.url.path, &self.sqlite) != noErr || self.sqlite == nil{
            throw NSError(domain: "数据库打开失败", code: 0)
        }
    }
    /// 创建数据库
    ///
    /// - Parameters:
    ///   - name: 数据库文件名 数据库在 checkDir下
    ///   - readonly: 是否只读
    ///   - mutex: 是否互斥
    public init(name:String,readonly:Bool = false,mutex:Bool = false) throws {
        let url = try Dapa.checkDir().appendingPathComponent(name)
        try self.init(url: url, readonly: readonly, mutex: mutex)
    }
    // MARK: hasable
    /// 比较数据库 数据库URL相等，数据库就相等
    /// - Parameters:
    ///   - lhs: 数据库1
    ///   - rhs: 数据库2
    /// - Returns: 相等结果
    public static func == (lhs: Dapa, rhs: Dapa) -> Bool {
        return lhs.url == rhs.url
    }
    
    public func hash(into hasher: inout Hasher) {
        hasher.combine(url)
    }
    // MARK: db api
    /// 查询表是否存在
    /// - Parameter name: 表名
    /// - Returns: 存在结果
    public func tableExist(name:String) throws->Bool{
        let rs = try self.prepare(sql: "PRAGMA table_info(\(name))")
        defer{
            rs.close()
        }
        if try rs.step() == .hasColumn{
            return true
        }else{
            return false
        }
    }
    /// 事务开始
    public func begin(){
        self.exec(sql: "begin")
    }
    /// 事务结束
    public func commit(){
        self.exec(sql: "commit")
    }
    /// 事物回滚
    public func rollback(){
        self.exec(sql: "rollback")
    }
    /// 执行sql
    /// - Parameter sql: sql 查询语句 支持参数，ResultSet 中可以添加
    /// - Returns: 结果集合迭代器
    public func prepare(sql:String) throws->Dapa.ResultSet{
        var stmt:OpaquePointer?
        #if DEBUG
            print(sql)
        #endif
        if sqlite3_prepare(self.sqlite!, sql, Int32(sql.utf8.count), &stmt, nil) != SQLITE_OK{
            throw NSError(domain: Dapa.errormsg(pointer: self.sqlite), code: 1)
        }
        return Dapa.ResultSet(sqlite: self.sqlite!, stmt: stmt!)
    }
    
    /// 数据库关闭
    public func close(){
        sqlite3_close(self.sqlite)
    }
    /// 自动创建数据库文件夹
    /// - Returns: 文件夹URL
    static public func checkDir() throws->URL{
        let name = Bundle.main.bundleIdentifier ?? "main" + ".Database"
        let url = try FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: true).appendingPathComponent(name)
        var b:ObjCBool = false
        let a = FileManager.default.fileExists(atPath: url.path, isDirectory: &b)
        if !(b.boolValue && a){
            try FileManager.default.createDirectory(at: url, withIntermediateDirectories: true, attributes: nil)
        }
        return url
    }
    /// 错误字符串
    /// - Parameter pointer: 数据库指针
    /// - Returns: 错误
    public static func errormsg(pointer:OpaquePointer?)->String{
        String(cString: sqlite3_errmsg(pointer))
    }
    /// 删除数据库
    public func deleteDatabaseFile(){
        do{
            try FileManager.default.removeItem(at: self.url)
        }catch{
            print(error)
        }
    }
 
    // MARK: Result set
    /// 数据库结果集合迭代器
    public struct ResultSet{
        // MARK: store property
        /// sqlite3 指针
        public let sqlite:OpaquePointer
        /// sqlite3 sql stmt
        public let stmt:OpaquePointer
        // MARK: init
        /// 创建结果集
        /// - Parameters:
        ///   - sqlite: sqlite3
        ///   - stmt: stmt
        public init(sqlite:OpaquePointer,stmt:OpaquePointer){
            self.sqlite = sqlite
            self.stmt = stmt
        }
        
        /// 重置
        public func reset(){
            sqlite3_reset(self.stmt)
        }
        
        // MARK: result set api
        /// 获取bind 参数的 下标
        /// - Parameter name: 参数名称 eg @abc,
        /// - Returns: 下标
        public func getParamIndexBy(name:String)->Int32{
            sqlite3_bind_parameter_index(self.stmt, name)
        }
        /// 获取参数名称
        /// - Parameter index: 参数下标
        /// - Returns: 参数名
        public func getParamNameBy(index:Int32)->String{
            String(cString: sqlite3_bind_parameter_name(self.stmt, index))
        }
        /// 参数个数
        public var paramCount:Int32{
            sqlite3_bind_parameter_count(self.stmt)
        }
        /// 绑定空参数
        /// - Parameter index: 下标
        public func bindNull(index:Int32) throws{
            if sqlite3_bind_null(self.stmt, index) != SQLITE_OK{
                throw NSError(domain: Dapa.errormsg(pointer: self.stmt), code: 4)
            }
        }
        /// 绑定空数据
        /// - Parameters:
        ///   - index: 下标
        ///   - blobSize: 空数据长度
        public func bindZero(index:Int32,blobSize:Int64) throws{
            if sqlite3_bind_zeroblob64(self.stmt, index, sqlite3_uint64(blobSize)) != SQLITE_OK{
                throw NSError(domain: Dapa.errormsg(pointer: self.stmt), code: 4)
            }
        }
        /// 绑定参数
        /// - Parameters:
        ///   - name: 参数名
        ///   - value: 参数 类型可以是 int32 int64 int double float string Data Date JSON
        public func bind<T>(name:String,value:T) throws{
            let i = self.getParamIndexBy(name: name)
            if(i > 0){
                try self.bind(index: i, value: value)
            }
        }
        /// 绑定参数
        /// - Parameters:
        ///   - index: 参数下标
        ///   - value: 参数 类型可以是 int32 int64 int double float string Data Date JSON
        public func bind<T>(index:Int32,value:T) throws{
            var flag:OSStatus = noErr
            if(value is Int32){
                flag = sqlite3_bind_int(self.stmt, index, value as! Int32)
            }else if(value is Int64){
                flag = sqlite3_bind_int64(self.stmt, index, value as! Int64)
            }else if (value is Int){
                if MemoryLayout<Int>.size == 8{
                    flag = sqlite3_bind_int64(self.stmt, index, Int64(value as! Int))
                }else{
                    flag = sqlite3_bind_int(self.stmt, index, Int32(value as! Int))
                }
            }else if(value is Double){
                flag = sqlite3_bind_double(self.stmt, index, value as! Double)
            }else if(value is Float){
                flag = sqlite3_bind_double(self.stmt, index, Double(value as! Float))
            }else if (value is String){
                let str = value as! String
                let c = str.cString(using: .utf8)
                let p = UnsafeMutablePointer<CChar>.allocate(capacity: str.utf8.count)
                memcpy(p, c, str.utf8.count)
                flag = sqlite3_bind_text(self.stmt, index, p, Int32(str.utf8.count)) { p in
                    p?.deallocate()
                }
            }else if (value is Data){
                let str = value as! Data
                let m:UnsafeMutablePointer<UInt8> = UnsafeMutablePointer.allocate(capacity: str.count)
                str.copyBytes(to: m, count: str.count)
                flag = sqlite3_bind_blob64(self.stmt, index, m, sqlite3_uint64(str.count), { p in p?.deallocate()})
            }else if (value is Date){
                flag = sqlite3_bind_double(self.stmt, index, (value as! Date).timeIntervalSince1970)
            }else{
                let data = try JSONSerialization.data(withJSONObject: value)
                try self.bind(index: index, value: data)
            }
            if(flag != noErr){
                throw NSError(domain: Dapa.errormsg(pointer: self.sqlite), code: 4)
            }
        }
        
        /// 读取一行
        /// - Returns: 读取结果
        @discardableResult
        public func step() throws ->ResultSet.StepResult{
            let rc = sqlite3_step(self.stmt)
            if rc == SQLITE_ROW{
                return .hasColumn
            }
            if rc == SQLITE_DONE{
                return .end
            }
            throw NSError(domain: Dapa.errormsg(pointer: self.sqlite), code: 2)
        }
        /// 结果的列名
        /// - Parameter index: 列下标
        /// - Returns: 列名
        public func columeName(index:Int32)->String{
            String(cString: sqlite3_column_name(self.stmt, index))
        }
        
        /// 读取Int
        /// - Parameter index: 列下表
        /// - Returns: Int 值
        public func columeInt(index:Int32)->Int{
            if MemoryLayout<Int>.size == 4{
                return Int(sqlite3_column_int(self.stmt, index))
            }else{
                return Int(sqlite3_column_int64(self.stmt, index))
            }
        }
        /// 读取Int64
        /// - Parameter index: 列下表
        /// - Returns: Int64 值
        public func columeInt64(index:Int32)->Int64{
            sqlite3_column_int64(self.stmt, index)
        }
        /// 读取Double
        /// - Parameter index:  列下表
        /// - Returns: double 值
        public func columeDouble(index:Int32)->Double{
            sqlite3_column_double(self.stmt, index)
        }
        /// 读取Float
        /// - Parameter index:  列下表
        /// - Returns: Float 值
        public func columeFloat(index:Int32)->Float{
            Float(sqlite3_column_double(self.stmt, index))
        }
        /// 读取
        /// - Parameter index: 下标
        /// - Returns: 字符串
        public func columeString(index:Int32)->String{
            guard let cstr = sqlite3_column_text(self.stmt, index) else {
                return ""
            }
            
            return String(cString: cstr)
        }
        
        /// 读取列
        /// - Parameters:
        ///   - index: 下标
        ///   - type: 类型
        /// - Returns: 数据
        public func colume<T>(index:Int32,type:CollumnDecType)->T?{
            let storeType = self.columeType(index: index)
            if(storeType == .nullCollumn){
                return nil
            }
            switch type{
            case .intDecType:
                return self.columeInt(index: index) as? T
            case .doubleDecType:
                return self.columeDouble(index: index) as? T
            case .textDecType:
                return self.columeString(index: index) as? T
            case .dataDecType:
                return self.columeData(index: index) as? T
            case .jsonDecType:
                return self.columeData(index: index) as? T
            case .dateDecType:
                return self.columeDate(index: index) as? T
            }
        }
        /// 读取列
        /// - Parameter index: 下标
        /// - Returns: 数据
        public func colume(index:Int32)->Any?{
            let type = self.columeDecType(index: index)
            let storeType = self.columeType(index: index)
            if(storeType == .nullCollumn){
                return nil
            }
            switch type{
            case .intDecType:
                return self.columeInt(index: index)
            case .doubleDecType:
                return self.columeDouble(index: index)
            case .textDecType:
                return self.columeString(index: index)
            case .dataDecType:
                return self.columeData(index: index)
            case .jsonDecType:
                let data =  self.columeData(index: index)
                guard let js = try? JSONSerialization.jsonObject(with: data) else { return nil }
                return js as Any
            case .dateDecType:
                return self.columeDate(index: index)
            case .none:
                return self.columeString(index: index)
            }
        }
        
        /// 读取日期
        /// - Parameter index: 下标
        /// - Returns: 日期参数
        public func columeDate(index:Int32)->Date{
            let time = sqlite3_column_double(self.stmt, index)
            return Date(timeIntervalSince1970: time)
        }
        /// 读取Data
        /// - Parameter index: 下标
        /// - Returns: Data数据
        public func columeData(index:Int32)->Data{
            let len = sqlite3_column_bytes(self.stmt, index)
            guard let byte = sqlite3_column_blob(self.stmt, index) else { return Data() }
            return Data(bytes: byte, count: Int(len))
        }
        /// 列参数类型是数据的原始类型
        /// - Parameter index: 下标
        /// - Returns: 列的类型
        public func columeType(index:Int32)->CollumnType{
            if sqlite3_column_type(self.stmt, index) == SQLITE_INTEGER{
                return .intCollumn
            }else if sqlite3_column_type(self.stmt, index) == SQLITE_FLOAT{
                return .doubleCollumn
            }else if sqlite3_column_type(self.stmt, index) == SQLITE_TEXT{
                return .textCollumn
            }else if sqlite3_column_type(self.stmt, index) == SQLITE_BLOB{
                return .dataCollumn
            }else if sqlite3_column_type(self.stmt, index) == SQLITE_NULL{
                return .nullCollumn
            }
            return .nullCollumn
        }
        /// 列参数定义类型，跟表定义有关
        /// - Parameter index: 下标
        /// - Returns: 类型
        public func columeDecType(index:Int32)->CollumnDecType?{
            guard let r = sqlite3_column_decltype(self.stmt, index) else {
                let type = self.columeType(index: index)
                switch(type){
                    
                case .nullCollumn:
                    return nil
                case .intCollumn:
                    return .intDecType
                case .doubleCollumn:
                    return .doubleDecType
                case .textCollumn:
                    return .textDecType
                case .dataCollumn:
                    return .dataDecType
                }
            }
            return CollumnDecType(rawValue: String(cString: r))
        }
        /// 列数
        public var columeCount:Int32{
            sqlite3_column_count(self.stmt)
        }
        /// 关闭
        public func close(){
            sqlite3_finalize(self.stmt)
        }
        /// 行读取的结果
        public enum StepResult{
            //有结果
            case hasColumn
            //无结果
            case end
        }
    }
}

extension Dapa.ResultSet{
    /// 绑定模型 模型参数名要是以@开头的
    /// - Parameter model: 模型
    public func bind<T:DapaResult>(model:T) throws{
        let  model = model.model
        for i in model{
            try self.bind(name: "@" + i.key, value: i.value)
        }
    }
    /// 读取数据到模型
    /// - Parameter model: 模型
    public func colume<T:DapaResult>(model:inout T){
        for i in 0 ..< self.columeCount{
            model.model[self.columeName(index: i)] = self.colume(index: i)
        }
    }
}





extension Dapa{
    // MARK: state
    /// 检查点模式
    public enum CheckPointMode:Int32{
        case PASSIVE = 0
        case FULL = 1
        case RESTART = 2
        case TRUNCATE = 3
    }
    
    /// 日志模式
    public enum JournalMode:String{
        case DELETE
        case TRUNCATE
        case PERSIST
        case MEMORY
        case WAL
        case OFF
    }
    
    /// 数据库日志同步模式
    public enum Synchronous:Int{
        case OFF = 0
        case NORMAL = 1
        case FULL = 2
        case EXTRA = 3
    }
    // MARK: db state
    /// 数据版本
    public var UserVersion:Int{
        get{
            do {
                let rs = try self.prepare(sql: "PRAGMA user_version")
                defer{
                    rs.close()
                }
                _ = try rs.step()
                let v = rs.columeInt(index: 0)
                
                return v
            }catch{
                return 0
            }
        }
        set{
            self.exec(sql: "PRAGMA user_version = \(newValue)")
        }
    }
    /// 更新行数
    public var changedRowNumber:Int32{
        return sqlite3_total_changes(self.sqlite)
    }
    /// 外键状态
    public var foreignKeys:Bool{
        set{
            self.exec(sql: "PRAGMA foreign_keys = \(newValue ? 1 : 0)")
        }
        get{
            do {
                let rs = try self.prepare(sql: "PRAGMA foreign_keys")
                defer{
                    rs.close()
                }
                _ = try rs.step()
                let v = rs.columeInt(index: 0) > 0
                
                return v
            }catch{
                return false
            }
            
        }
    }
    
    /// 日志模式
    public var journalMode:JournalMode{
        get{
            do{
                let rs = try self.prepare(sql: "PRAGMA journal_mode")
                defer {
                    rs.close()
                }
                try rs.step()
                let jm = rs.columeString(index: 0)
                return JournalMode(rawValue: jm) ?? .OFF
            }catch{
                return .OFF
            }
        }
        set{
            self.exec(sql: "PRAGMA journal_mode = \(newValue)")
        }
    }
    
    /// 同步模式
    public var synchronous:Synchronous{
        get{
            do{
                let rs = try self.prepare(sql: "PRAGMA synchronous")
                defer {
                    rs.close()
                }
                try rs.step()
                let jm = rs.columeInt(index: 0)
                return Synchronous(rawValue: jm) ?? .OFF
            }catch{
                return .OFF
            }
        }
        
        set{
            self.exec(sql: "PRAGMA synchronous = \(newValue)")
        }
    }
    
    /// 执行日志检查点 特定的模式执行 默认参数是 FULL
    /// - Parameter mode: 检查点模式
    public func checkPoint(mode:CheckPointMode = .FULL){
        if SQLITE_OK == sqlite3_wal_checkpoint_v2(self.sqlite, nil, mode.rawValue, nil, nil){
            print("check point ok")
        }
        sqlite3_os_init()
    }
    
    /// 设置自动检查点 日志中有指点的页数自动执行检查点
    /// - Parameter n: 自动检查点额页数
    public func autoCheckPoint(n:Int32 = 10000){
        sqlite3_wal_autocheckpoint(self.sqlite, n)
    }
}

extension Dapa{
    /// 注册数据库函数
    /// - Parameter function: 数据库函数
    public func registerFunction(function:Funtion){
        if function.xFunc != nil{
            sqlite3_create_function_v2(self.sqlite, function.name, function.nArg, SQLITE_UTF8, Unmanaged<Funtion>.passRetained(function).toOpaque(), { ctx, n, params in
                let df = Unmanaged<Funtion>.fromOpaque(sqlite3_user_data(ctx))
                df.takeUnretainedValue().xFunc?(FunctionContext(ctx: ctx!),Dapa.makeParam(n: n, param: params))
            }, nil, nil,{app in
                guard let point = app else { return }
                Unmanaged<Funtion>.fromOpaque(point).release()
            })
        }else{
            sqlite3_create_function_v2(self.sqlite, function.name, function.nArg, SQLITE_UTF8, Unmanaged<Funtion>.passRetained(function).toOpaque(), nil, { ctx, n, params in
                let df = Unmanaged<Funtion>.fromOpaque(sqlite3_user_data(ctx))
                df.takeUnretainedValue().xStep?(FunctionContext(ctx: ctx!),Dapa.makeParam(n: n, param: params))
            },{ctx in
                let df = Unmanaged<Funtion>.fromOpaque(sqlite3_user_data(ctx))
                df.takeUnretainedValue().xFinal?(FunctionContext(ctx: ctx!))
            },{app in
                guard let point = app else { return }
                Unmanaged<Funtion>.fromOpaque(point).release()
            })
        }

    }
    private static func makeParam(n:Int32,param:UnsafeMutablePointer<OpaquePointer?>?)->[Dapa.Value]{
        var array:[Dapa.Value] = []
        for i in 0 ..< n{
            guard let ptr = param?.advanced(by: Int(i)).pointee else {
                continue
            }
            array.append(Dapa.Value(sqlValue: ptr))
        }
        return array
    }
}

extension Dapa{
    
    
    /// 数据库查询结果 用于 exec 方法
    public class Result:CustomStringConvertible{
        public var description: String{
            return "\(self.result)"
        }
        /// 结果的行
        @dynamicMemberLookup
        public struct Row:CustomStringConvertible{
            public var description: String{
                return "\(self.colume)"
            }
            
            fileprivate var colume:[String:String]
            public subscript(dynamicMember dynamicMember:String)->String?{
                return colume[dynamicMember]
            }
            fileprivate mutating func load(argc:Int32,
                             argv:UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?,
                             col:UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>?){
                for i in 0 ..< argc{
                    guard let k = col?.advanced(by: Int(i)).pointee else { continue }
                    guard let v = argv?.advanced(by: Int(i)).pointee else { continue }
                    
                    self.colume[String(cString: k)] = String(cString: v)
                }
            }
        }
        public var result:[Row] = []
    }
    /// 执行sql 语句
    /// - Parameter sql: sql 语句 不支持参数绑定
    /// - Returns: 查询结果
    @discardableResult
    public func exec(sql:String)->Dapa.Result{
        #if DEBUG
            print(sql)
        #endif

        let pointer = Dapa.Result()
        let um = Unmanaged<Dapa.Result>.passUnretained(pointer)
        sqlite3_exec(self.sqlite, sql, { data, argc, argv, col in
            let dr = Unmanaged<Dapa.Result>.fromOpaque(data!).takeUnretainedValue()
            var drc = Dapa.Result.Row(colume: [:])
            drc.load(argc: argc, argv: argv, col: col)
            dr.result.append(drc)
            return 0
        },um.toOpaque() , nil)
        
        return pointer
    
    }
}

// json 解码
public let DapaJsonDecoder:JSONDecoder = {
    let json = JSONDecoder();
    if #available(iOS 15.0,macOS 12.0, *) {
        json.allowsJSON5 = true
    } else {

    };
    return json
}()
// json 编码
public let DapaJsonEncoder:JSONEncoder = {
    let json = JSONEncoder()
    return json
}()
