package khat

import org.postgresql.ds.PGPoolingDataSource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import kotlin.jdbc.*
import org.slf4j.LoggerFactory
import java.io.StringReader
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.reflect.Constructor
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import javax.json.Json
import javax.json.JsonObject
import javax.sql.DataSource
import kotlin.reflect.KClass

@Retention(RetentionPolicy.RUNTIME)
annotation class table(val path: String = "")

interface ResultSetMapper<M> {
    fun map(rs: ResultSet): M
}

class DataClassResultSetMapper<M> (modelClass: java.lang.Class<M>)
    : ResultSetMapper<M> {

    private val constructor: Constructor<M>
    private val fields: List<Pair<String, Class<*>>>

    init {
        val constructors = modelClass.getDeclaredConstructors()

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructor = constructors[0] as Constructor<M>

        fields = modelClass.getDeclaredFields()
                .map { Pair(it.getName(), it.getType()) }
                .filterNot { it.first.equals("\$kotlinClass") }
    }

    override fun map(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.first)
            val type = it.second
            when {
                type.isAssignableFrom(javaClass<JsonObject>()) -> {
                    val reader = Json.createReader(StringReader(value.toString()))
                    reader.read() as JsonObject
                }
                else -> value
            }

        }.toTypedArray()
        return constructor.newInstance(*args)
    }

}

open class Dao<M, in K>(val dataSource: DS) {

    val mapper: ResultSetMapper<M>
    val tableName: String

    init {
        tableName = this.javaClass.getSimpleName().toLowerCase()
        mapper = DataClassResultSetMapper<M>(getModelType() as java.lang.Class<M>)
    }

    fun withId(id: K) : M? {
        val connection = this.dataSource.get().getConnection()
        val statement = connection.prepareStatement("select * from ${tableName} where id = ?")
        statement.setObject(1, id)
        val rs = statement.executeQuery();
        if (!rs.next()) {
            return null
        } else {
            return this.mapper.map(rs)
        }
    }

    fun findWhere(sql: String, vararg args: Any): List<M> {
        val result = arrayListOf<M>()
        this.dataSource.get().query("select * from ${tableName}", {
            for (row in it) {
                result.add(this.mapper.map(row))
            }
        })
        return result
    }

    private fun getParametrizedTypes(): Array<Type> {
        val types = (this.javaClass.getGenericSuperclass() as ParameterizedType).getActualTypeArguments()
        if (types.size() < 2) {
            throw IllegalStateException("Can't use a DAO without concrete types")
        }
        return types
    }

    private fun getModelType(): Type {
        return getParametrizedTypes().get(0)
    }

    private fun getKeyType(): Type {
        return getParametrizedTypes().get(0)
    }
}

object DS {
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

    fun get(): DataSource {
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
data class Record(
        val id: UUID,
        val doc: JsonObject
)

object Users : Dao<User, Int>(DS)
object Records: Dao<Record, UUID>(DS)

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger(::main.javaClass);
    
    val users = Users.findWhere("name = ?", "Jerome")
    users.map({ user ->
        println(user)
    })

    println("> With id 3 ->")

    Users.withId(3).let { println(it) }

    Records.withId(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")).let { println(it) }

}
