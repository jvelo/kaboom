package khat

import org.postgresql.ds.PGPoolingDataSource
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.function.Supplier
import javax.json.JsonObject
import javax.sql.DataSource
import kotlin.properties.Delegates

object DataSourceSupplier : Supplier<DataSource> {
    private val source: PGPoolingDataSource

    init {
        source = PGPoolingDataSource();
        this.initialize();
    }

    fun initialize() {
        source.setDataSourceName("A Data Source");
        source.setServerName("localhost");
        source.setDatabaseName("test");
        source.setUser("postgres");
        source.setPassword("");
        source.setMaxConnections(10);
    }

    override fun get(): DataSource {
        return this.source
    }
}

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
data class Record(
        val id: UUID,
        column("doc") val json: JsonObject
) {
    val a: String by Delegates.mapVal(json)
}

data class Test(json: Map<String, Any?>) {
    val bar: String by Delegates.mapVal(json)

    val foo by Delegates.blockingLazy {
        this.bar.length()
    }
}

object Users : Dao<User, Int>(DataSourceSupplier)

object Records : Dao<Record, UUID>(DataSourceSupplier)

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger(::main.javaClass);

    val users = Users.findWhere("name = ?", "Jerome")
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

    Records.withId(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")).let {
        println(it)
    }

}
