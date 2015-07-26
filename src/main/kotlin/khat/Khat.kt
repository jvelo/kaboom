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

interface ResultSetMapper<out M> {
    fun map(rs: ResultSet): M
}

class DataClassResultSetMapper<out M>
    (klass: java.lang.Class<M>)
    : ResultSetMapper<M> {

    init {

    }

    override fun map(rs: ResultSet): M {
        throw RuntimeException()
    }

}

open class Dao<out M, in K>(val dataSource: DS) {

    fun withId(id: K) : M? {

    }

    fun findWhere(sql: String, vararg args: Any): List<M> {
        val tableName = this.javaClass.getSimpleName().toLowerCase()
        val constructor = this.getModelConstructor()
        val fieldNames = this.getFieldNames()
        val result = arrayListOf<M>()
        this.dataSource.get().query("select * from ${tableName}", {
            for (row in it) {
                val args = fieldNames.map { row.get(it) }.toTypedArray()
                val m : M = constructor.newInstance(*args)
                result.add(m)
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

    private fun getFieldNames() = (getModelType() as java.lang.Class<*>).getDeclaredFields()
            .map { it.getName() }
            .filterNot { it.equals("\$kotlinClass") }

    private fun getModelConstructor(): Constructor<M> {
        val constructors = (getModelType() as java.lang.Class<*>).getDeclaredConstructors()
        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }
        return constructors[0] as Constructor<M>
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
        println(user.id)
        println(user.name)
        println(user.password)
    })

}
