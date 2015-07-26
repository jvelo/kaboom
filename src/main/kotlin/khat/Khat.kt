package khat

import org.postgresql.ds.PGPoolingDataSource
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.util.*
import kotlin.jdbc.*
import org.slf4j.LoggerFactory
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.reflect.Constructor
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import javax.sql.DataSource

@Retention(RetentionPolicy.RUNTIME)
annotation class table(val path: String = "")

interface ResultSetMapper<M> {
    fun map(rs: ResultSet): M
}

class DataClassResultSetMapper<M> (modelClass: java.lang.Class<M>)
    : ResultSetMapper<M> {

    private val constructor: Constructor<M>
    private val fieldNames: List<String>

    init {
        val constructors = modelClass.getDeclaredConstructors()

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructor = constructors[0] as Constructor<M>

        fieldNames = modelClass.getDeclaredFields()
                .map { it.getName() }
                .filterNot { it.equals("\$kotlinClass") }
    }

    override fun map(rs: ResultSet): M {
        val args = fieldNames.map { rs.get(it) }.toTypedArray()
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

object Users : Dao<User, Int>(DS)

fun main(args: Array<String>) {

    val logger = LoggerFactory.getLogger(::main.javaClass);
    val users = Users.findWhere("name = ?", "Jerome")

    users.map({ user ->
        println(user)
    })

    println("> With id 3 ->")

    Users.withId(3).let { println(it) }

}
