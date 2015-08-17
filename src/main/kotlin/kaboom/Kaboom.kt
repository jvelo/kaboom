package kaboom

import kaboom.dao.Dao
import kaboom.types.registerDefaultTypesMappers
import org.postgresql.ds.PGPoolingDataSource
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.json.JsonObject
import javax.json.JsonString
import javax.sql.DataSource
import kotlin.properties.Delegates

object DataSourceSupplier : () -> DataSource {
    private val source: PGPoolingDataSource

    init {
        source = PGPoolingDataSource();
        this.initialize();
    }

    fun initialize() {
        source setDataSourceName "A Data Source"
        source setServerName "localhost"
        source setDatabaseName "test"
        source setUser "postgres"
        source setPassword ""
        source setMaxConnections 10
    }

    override fun invoke(): DataSource {
        return this.source
    }
}

table("users")
data class User(
        val id: Long,
        val name: String,
        val password: String
)

// CREATE TABLE records (id uuid, doc jsonb)
// insert into records values ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": "b", "c": 2}');
// insert into records values ('3c75d791-a654-421c-b442-768a4748dff4', '{"toto":  "tata"}');

table("records")
filter("doc @> '{\"a\":\"b\"}'")
data class Document(
        val id: UUID,
        column("doc") val json: JsonObject
) {
    val a: JsonString by Delegates.mapVal(json)

    val b: String
        get() { return a.toString() }
}

data class Test(json: Map<String, Any?>) {
    val bar: String by Delegates.mapVal(json)

    val foo by Delegates.blockingLazy {
        this.bar.length()
    }
}


object Users: Dao<User, Int>(DataSourceSupplier)
object Documents : Dao<Document, UUID>(DataSourceSupplier)

fun main(args: Array<String>) {

    registerDefaultTypesMappers()

    val logger = LoggerFactory.getLogger(::main.javaClass);

    val users = Users.where("name = ?", "Jerome")
    users.map({ user ->
        println(user)
    })

    println("> With id 3 ->")

    Users.withId(3).let { println(it) }

    val jers = Users.query()
            .where("name like '%' || ? || '%'")
            .argument("Jer")
            .limit(5)
            .execute()

    val document = Documents.withId(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"))
    document?.let {
        println(it)
        println(it.a)
        println(it.b)
    }

}
