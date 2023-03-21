import XCTest
@testable import Dapa
import SQLite3

final class DapaTests: XCTestCase {
    
    func testCreate() throws {
        let db = try Database(name: "db")
        Member.create(db: db)
        MemberOnline.create(db: db)
        MemberRelation.create(db: db)
        MemberDisplay.View(db: db)
        MemberCanVisible.View(db: db)
    }
    
    func testInsert() throws{
        let db = try Database(name: "db")
        for i in 0 ..< 100{
            var mem = Member()
            mem.domain = "\(i)"
            mem.username = "name \(i)"
            mem.remark = "remark \(i)"
            mem.avatar = "avatar \(i)"
            try mem.insert(db: db)
            
            var onl = MemberOnline()
            onl.domain = mem.domain
            onl.online = (i % 2 == 0 ? 1 : 0)
            try onl.insert(db: db)
        }
        db.close()
    }
    func testRelation() throws{
        let db = try Database(name: "db")
        for i in 0 ..< 100{
            var rl = MemberRelation()
            
            rl.domain1 = "\(i)"
            rl.domain2 = "\(Int.random(in: 0 ..< 100))"
            try? rl.insert(db: db)
        }
        db.close()
    }
    func testSelect() throws {
        
        let db = try Database(name: "db",readonly: true)
        db.registerFunction(function: DatabaseFuntion(name: "go", nArg: 1,xFunc: { ctx, params in
            ctx.result(value: 89)
        }))
        
        
        let q = DatabaseGenerator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = 89") .and(condition: "Member.domain = MemberRelation.domain1")
        let user:[MemberStaticDisplay] = try MemberStaticDisplay.query(condition: q,groupBy: ["domain1"]).query(db: db)
        
//        let st:[MemberDisplay] = try MemberDisplay.select(db: db,condtion: "domain2=89")
        let st:[MemberDisplay] = try MemberDisplay.query(condition: "domain2=go(80)").query(db: db)
        print(user)
        print(st)
        db.close()
    }
    
    func testMMM() throws{
        let db = try Database()
        
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
        let q = DatabaseGenerator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = 89") .and(condition: "Member.domain = MemberRelation.domain1")
        let user:[MemberStaticDisplay] = try MemberStaticDisplay.query(condition: q,groupBy: ["domain1"]).query(db: db)
        
//        let st:[MemberDisplay] = try MemberDisplay.select(db: db,condtion: "domain2=89")
        let st:[MemberDisplay] = try MemberDisplay.query(condition: "domain2=89").query(db: db)
        let backup = try DatabaseBackup(name: "mmm", database: db)
        backup.backup()
        db.close()
        
    }
    
    func testMK() throws{
        let db = try Database(name: "db",readonly: true)
        db.registerFunction(function: DatabaseFuntion(name: "mm", nArg: 1,xStep: { ctx, param in
            let a:UnsafeMutablePointer<Int64>? = ctx.pointer()
            let v:Int64 = param.first?.value() ?? 0
            a?.pointee += v
            
        },xFinal: { ctx in
            guard let a:UnsafeMutablePointer<Int64> = ctx.pointer() else {
                ctx.result(value: 0)
                return
            }
            ctx.result(value: a.pointee)
        }))
        self.measure {
            
            
            let result = db.exec(sql: "select mm(domain),remark from Member")
            
            print(result)
        }
        db.close()
    }
}

public struct Member:DatabaseModel{
    public static var tableName: String = "Member"
    
    public static var declare: [Dapa.DatabaseColumeDeclare] {
        [
            DatabaseColumeDeclare(name: "domain", type: .textDecType,primary: true),
            DatabaseColumeDeclare(name: "username", type: .textDecType),
            DatabaseColumeDeclare(name: "remark", type: .textDecType),
            DatabaseColumeDeclare(name: "avatar", type: .textDecType)
            
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberOnline:DatabaseModel{
    public static var tableName: String = "MemberOnline"
    
    public static var declare: [Dapa.DatabaseColumeDeclare] {
        [
            DatabaseColumeDeclare(name: "domain", type: .textDecType,primary: true),
            DatabaseColumeDeclare(name: "online", type: .intDecType)
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberRelation:DatabaseModel{
    public static var tableName: String = "MemberRelation"
    
    public static var declare: [Dapa.DatabaseColumeDeclare] {
        [
            DatabaseColumeDeclare(name: "domain1", type: .textDecType,primary: true),
            DatabaseColumeDeclare(name: "domain2", type: .textDecType,primary: true)
        ]
    }
    
    public init () {}
    
    public var model: Dictionary<String, Any> = [:]
    
}

public struct MemberDisplay:DatabaseViewModel{
    public static var groupBy: [String] = ["domain1"]
    
    
    public static var view: Dapa.DatabaseGenerator.ItemName {
        DatabaseGenerator.ItemName.name(name: "MemberDisplay")
    }

    public static var condition: Dapa.DatabaseGenerator.DatabaseCondition?{
        
        return DatabaseGenerator.DatabaseCondition(stringLiteral: "MemberOnline.domain == Member.domain").and(condition: "MemberRelation.domain2 = 89") .and(condition: "Member.domain = MemberRelation.domain1")
    }
    
    public init() {
        self.model = [:]
    }
    
    public static var queryDeclare: [Dapa.DatabaseQueryColumeDeclare] = [
        .init(name: "MemberOnline.domain", type: .textDecType),
        .init(name: "username", type: .textDecType),
        .init(name: "remark", type: .textDecType),
        .init(name: "avatar", type: .textDecType),
        .init(name: "online", type: .textDecType),
        .init(name: "domain2", type: .textDecType),
    ]
    
    public static var table: Dapa.DatabaseGenerator.Select.JoinTable{
        .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline")).join(type: .join, table: .name(name: "MemberRelation"))
    }
    
    public var model: Dictionary<String, Any>
    
}
public struct MemberCanVisible:DatabaseViewModel{
    public static var groupBy: [String] = []
    
    
    public static var view: Dapa.DatabaseGenerator.ItemName {
        DatabaseGenerator.ItemName.name(name: "MemberCanVisible")
    }
    
    public static var condition: Dapa.DatabaseGenerator.DatabaseCondition?{
        
        return DatabaseGenerator.DatabaseCondition(stringLiteral: "MemberOnline.domain = Member.domain").and(condition: "MemberOnline.online = 1")
    }
    
    public init() {
        self.model = [:]
    }
    
    public static var queryDeclare: [Dapa.DatabaseQueryColumeDeclare] = [
        .init(name: "MemberOnline.domain", type: .textDecType),
        .init(name: "username", type: .textDecType),
        .init(name: "remark", type: .textDecType),
        .init(name: "avatar", type: .textDecType),
        .init(name: "online", type: .textDecType),
    ]
    
    public static var table: Dapa.DatabaseGenerator.Select.JoinTable{
        .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline"))
    }
    
    public var model: Dictionary<String, Any>
    
}
public struct MemberStaticDisplay:DatabaseQueryWrapModel{
    
    public static var table: Dapa.DatabaseGenerator.Select.JoinTable{
        .init(table: .name(name: "Member")).join(type: .join, table: .name(name: "MemberOnline")).join(type: .join, table: .name(name: "MemberRelation"))
    }
    
    public init(){ }
    
    @DapaQueryColume(name:"MemberOnline.domain",colume: "domain",type: .textDecType)
    public var domain:String = ""
    @DapaQueryColume(name: "username",type: .textDecType)
    public var username:String = ""
    @DapaQueryColume(name: "remark",type: .textDecType)
    public var remark:String = ""
    @DapaQueryColume(name: "avatar",type: .textDecType)
    public var avatar:String = ""
    @DapaQueryColume(name: "online",type: .textDecType)
    public var online:String = ""

}
