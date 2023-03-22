import XCTest
@testable import Dapa
import SQLite3

final class DapaTests: XCTestCase {
    
    func testCreate() throws {
        let db = try Dapa(name: "db")
        Member.create(db: db)
        MemberOnline.create(db: db)
        MemberRelation.create(db: db)
        MemberDisplay.View(db: db)
        MemberCanVisible.View(db: db)
    }
    func testInsert() throws{
        let db = try Dapa(name: "db")
        let ob = Dapa.Observer(dapa: db) { ev, db, table, row in
            print(ev,db,table,row)
        }
        for i in 0 ..< 100{
            var mem = Member()
            mem.domain = "\(i)"
            mem.username = "name \(i)"
            mem.remark = "remark \(i)"
            mem.avatar = "avatar \(i)"
            try mem.insert(db: db)
            ob.close()
            var onl = MemberOnline()
            onl.domain = mem.domain
            onl.online = (i % 2 == 0 ? 1 : 0)
            try onl.insert(db: db)
        }
        db.close()
        print(ob)
    }
    func testRelation() throws{
        let db = try Dapa(name: "db")
        for i in 0 ..< 100{
            var rl = MemberRelation()
            
            rl.domain1 = "\(i)"
            rl.domain2 = "\(Int.random(in: 0 ..< 100))"
            try? rl.insert(db: db)
        }
        db.close()
    }
    func testSelect() throws {
        
        let db = try Dapa(name: "db",readonly: true)
        db.registerFunction(function: Dapa.Funtion(name: "go", nArg: 1,xFunc: { ctx, params in
            let param:Int = params.first!.value()
            ctx.result(value: param + 1)
        }))
        
        
        let condition = Dapa.Generator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = @user2") .and(condition: "Member.domain = MemberRelation.domain1")
        let user:[MemberStaticDisplay] = try MemberStaticDisplay.query(condition: condition,groupBy: ["domain1"]).query(db: db,param: ["@user2":10])
        
//        let st:[MemberDisplay] = try MemberDisplay.select(db: db,condtion: "domain2=89")
        let st:[MemberDisplay] = try MemberDisplay.query(condition: "domain2=go(@test)").query(db: db,param: ["@test":"88"])
        print(user)
        print(st)
        db.close()
    }
    
    func testMMM() throws{
        let db = try Dapa()
        
        Member.create(db: db)
        MemberOnline.create(db: db)
        MemberRelation.create(db: db)
        MemberDisplay.View(db: db)
        MemberCanVisible.View(db: db)
        for i in 0 ..< 100{
            var mem = Member()
            mem.domain = "\(i)"
            mem.username = "name \(i)"
            mem.remark = "remark \(i)"
            mem.avatar = "avatar \(i)"
            try mem.insert(db: db)
            
            var onl = MemberOnline()
            onl.domain = mem.domain
            onl.online = Int.random(in: 0 ..< 2)
            try onl.insert(db: db)
        }
        for i in 0 ..< 100{
            var rl = MemberRelation()
            
            rl.domain1 = "\(i)"
            rl.domain2 = "\(Int.random(in: 0 ..< 100))"
            try? rl.insert(db: db)
        }
        let condition = Dapa.Generator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = 89") .and(condition: "Member.domain = MemberRelation.domain1")
        let user:[MemberStaticDisplay] = try MemberStaticDisplay.query(condition: condition,groupBy: ["domain1"]).query(db: db)
        
//        let st:[MemberDisplay] = try MemberDisplay.select(db: db,condtion: "domain2=89")
        let st:[MemberDisplay] = try MemberDisplay.query(condition: "domain2=89").query(db: db)
        let backup = try Dapa.Backup(name: "mmm", database: db)
        backup.backup()
        db.close()
    }
    
    func testMK() throws{
        let queue = try DapaNormalQueue(name: "db")
        queue.query { db in
            let result = db.exec(sql: "select * from Member")
            print(result)
        }
        Thread.sleep(forTimeInterval: 10)
    }
    func testStatic () throws{
        let db = try Dapa(name: "db")
        db.exec(sql: "drop table \(MemberStatic.tableName)")
        MemberStatic.create(db: db)
        let a = MemberStatic()
        a.avatar = "dasdasd"
        a.domain = "1"
        a.remark = "dasdadadadadadadadadsa"
        a.username = "Dadadadad44"
        try a.insert(db: db)
        var b:MemberStatic = MemberStatic()
        b.domain = "1"
        try b.sync(db: db)
        print(a)
        
        db.close()
    }
    func testMk() throws{
        let db = try Dapa(name: "db")
        TestRowId.create(db: db)
        for i in 0 ..< 100{
            let tr = TestRowId();
            tr.domain = "dadads\(i)"
            try tr.insert(db: db)
        }
        let trr:[TestRowId] = try TestRowId.select().query(db: db)
        
        var rr = TestRowId();
        rr.rowid = 99
        try rr.sync(db: db)
        rr.domain = "change 9999"
        try rr.update(db: db)
        print(rr)
    }
    
}

public struct Member:DapaModel{
    public static var tableName: String = "Member"
    
    public static var declare: [Dapa.ColumeDeclare] {
        [
            Dapa.ColumeDeclare(name: "domain", type: .textDecType,primary: true),
            Dapa.ColumeDeclare(name: "username", type: .textDecType),
            Dapa.ColumeDeclare(name: "remark", type: .textDecType),
            Dapa.ColumeDeclare(name: "avatar", type: .textDecType)
            
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberOnline:DapaModel{
    public static var tableName: String = "MemberOnline"
    
    public static var declare: [Dapa.ColumeDeclare] {
        [
            Dapa.ColumeDeclare(name: "domain", type: .textDecType,primary: true),
            Dapa.ColumeDeclare(name: "online", type: .intDecType)
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberRelation:DapaModel{
    public static var tableName: String = "MemberRelation"
    
    public static var declare: [Dapa.ColumeDeclare] {
        [
            Dapa.ColumeDeclare(name: "domain1", type: .textDecType,primary: true),
            Dapa.ColumeDeclare(name: "domain2", type: .textDecType,primary: true)
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberDisplay:DapaViewModel{
    public static var groupBy: [String] = ["domain1"]
    
    
    public static var view: Dapa.Generator.ItemName {
        Dapa.Generator.ItemName.name(name: "MemberDisplay")
    }

    public static var condition: Dapa.Generator.DatabaseCondition?{
        
        return Dapa.Generator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = 89") .and(condition: "Member.domain = MemberRelation.domain1")
    }
    
    public init() {
        self.model = [:]
    }
    
    public static var queryDeclare: [Dapa.QueryColumeDeclare] = [
        .init(name: "MemberOnline.domain", type: .textDecType),
        .init(name: "username", type: .textDecType),
        .init(name: "remark", type: .textDecType),
        .init(name: "avatar", type: .textDecType),
        .init(name: "online", type: .textDecType),
        .init(name: "domain2", type: .textDecType),
    ]
    
    public static var table: Dapa.Generator.Select.JoinTable{
        .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline")).join(type: .join, table: .name(name: "MemberRelation"))
    }
    
    public var model: Dictionary<String, Any>
    
}
public struct MemberCanVisible:DapaViewModel{
    public static var groupBy: [String] = []
    
    
    public static var view: Dapa.Generator.ItemName {
        Dapa.Generator.ItemName.name(name: "MemberCanVisible")
    }
    
    public static var condition: Dapa.Generator.DatabaseCondition?{
        
        return Dapa.Generator.DatabaseCondition(stringLiteral: "MemberOnline.domain = Member.domain").and(condition: "MemberOnline.online = 1")
    }
    
    public init() {
        self.model = [:]
    }
    
    public static var queryDeclare: [Dapa.QueryColumeDeclare] = [
        .init(name: "MemberOnline.domain", type: .textDecType),
        .init(name: "username", type: .textDecType),
        .init(name: "remark", type: .textDecType),
        .init(name: "avatar", type: .textDecType),
        .init(name: "online", type: .textDecType),
    ]
    
    public static var table: Dapa.Generator.Select.JoinTable{
        .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline"))
    }
    
    public var model: Dictionary<String, Any>
    
}


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

public struct TestRowId:DapaWrapModel{
    public static var tableName: String{
        "TestRowId"
    }
    
    public init() {}
    
    @Dapa.DapaColume(type: .textDecType)
    public var domain:String = ""
    
    @Dapa.DapaRowId
    public var rowid:Int64 = 0
    
}
