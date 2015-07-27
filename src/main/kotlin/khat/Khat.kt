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
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import javax.json.Json
import javax.json.JsonObject
import javax.sql.DataSource
import kotlin.properties.Delegates
import kotlin.reflect.KClass

@Retention(RetentionPolicy.RUNTIME)
annotation class table(val name: String = "")

@Retention(RetentionPolicy.RUNTIME)
annotation class column(val name: String)

interface ResultSetMapper<M> {
    fun map(rs: ResultSet): M
}

class DataClassResultSetMapper<M>(modelClass: java.lang.Class<M>)
: ResultSetMapper<M> {

    class ColumnField(
        val fieldName: String,
        val fieldClass: Class<*>,
        val columnName: String? = null
    )

    private val constructor: Constructor<M>
    private val fields: List<ColumnField>

    suppress("UNCHECKED_CAST")
    init {
        val constructors = modelClass.getDeclaredConstructors()

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructor = constructors[0] as Constructor<M>

        fields = modelClass.getDeclaredFields()
                .filterNot { it.getName().equals("\$kotlinClass") }
                .mapIndexed { index, field -> ColumnField(field.getName(), field.getType(), getColumnName(index)) }

    }

    override fun map(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName ?: it.fieldName)
            val type = it.fieldClass
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

    private fun getColumnName(index: Int): String? {
        val annotations = constructor.getParameterAnnotations()[index]
        return annotations?.singleOrNull {
            it?.annotationType()?.equals(javaClass<column>()) ?: false
        }?.let {
            (it as column).name
        }
    }

}

data class Query(
        val select: String,
        val where: String? = null,
        val order: String? = null,
        val limit: Long? = null,
        val offset: Long? = null,
        val arguments: List<Any> = listOf()
)

class QueryBuilder<M>(val dataSource: DS, val mapper: ResultSetMapper<M>, val query: Query) {

    fun where(where: String) = QueryBuilder<M>(dataSource, mapper, query.copy(where = where))

    fun limit(limit: Long) = QueryBuilder<M>(dataSource, mapper, query.copy(limit = limit))

    fun offset(offset: Long) = QueryBuilder<M>(dataSource, mapper, query.copy(offset = offset))

    fun order(order: String) = QueryBuilder<M>(dataSource, mapper, query.copy(order = order))

    fun argument(argument: Any) = QueryBuilder<M>(dataSource, mapper, query.copy(arguments = this.query.arguments.plus(argument)))

    fun arguments(vararg arguments: Any) = QueryBuilder<M>(dataSource, mapper, query.copy(arguments = this.query.arguments.plus(arguments)))

    fun execute(): List<M> {
        println(serialize())
        val rs = getResultSet()
        val result = arrayListOf<M>()
        while (rs.next()) {
            result.add(this.mapper.map(rs))
        }
        return result
    }

    suppress("BASE_WITH_NULLABLE_UPPER_BOUND")
    fun single(): M? {
        println(serialize())
        val rs = getResultSet()
        if (!rs.next()) {
            return null
        } else {
            return this.mapper.map(rs)
        }
    }

    private fun getResultSet(): ResultSet {
        val sql = serialize()
        val connection = dataSource.get().getConnection()
        val statement = connection.prepareStatement(sql)
        query.arguments.forEachIndexed { i, argument ->
            statement.setObject(i + 1, argument)
        }
        val rs = statement.executeQuery();
        return rs
    }

    private fun serialize(): String = query.select +
            (if (query.where != null) " WHERE ${query.where}" else "") +
            (if (query.order != null) " ORDER BY ${query.order}" else "") +
            (if (query.limit != null) " LIMIT ${query.limit}" else "") +
            (if (query.offset != null) " LIMIT ${query.offset}" else "")
}

open class Dao<M, in K>(val dataSource: DS) {

    val mapper: ResultSetMapper<M>
    val tableName: String

    suppress("UNCHECKED_CAST")
    init {
        tableName = this.javaClass.getSimpleName().toLowerCase()
        mapper = DataClassResultSetMapper<M>(getModelType() as java.lang.Class<M>)
    }

    fun query(): QueryBuilder<M> {
        return QueryBuilder(dataSource, mapper, Query(select = "select * from ${tableName}"))
    }

    suppress("BASE_WITH_NULLABLE_UPPER_BOUND")
    fun withId(id: K): M? = query().where("id = ?").argument(id).single()

    fun findWhere(sql: String, vararg args: Any): List<M> {
        val query = query().where(sql)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.execute()
    }

    // ---------------------------------------------------------------------------------------------

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
table("records")
data class Record(
        val id: UUID,
        column("doc") val json: JsonObject
) {
}

data class Test(json: Map<String, Any?>) {
    val bar: String by Delegates.mapVal(json)

    val foo by Delegates.blockingLazy {
        this.bar.length()
    }
}

object Users : Dao<User, Int>(DS)

object Records : Dao<Record, UUID>(DS)

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

    Records.withId(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")).let { println(it) }

}
