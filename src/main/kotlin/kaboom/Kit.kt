package kaboom

import kaboom.driver.DefaultDriver
import kaboom.driver.Driver
import kaboom.jdbc.set
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource

/**
 * Statement argument value wrapper that adds a possible type hint to a value.
 * The type hint is a DB-type hint meant to help drivers create an appropriate object before sending
 * it to the database. This is useful for values which DB type we can't guess in advance and require
 * DB-specific treatment : like "json" vs "jsonb" vs "hstore" for a Map<String, Object> value in PG.
 */
public data class Argument(val value: Any?, val hint: String? = null)

public class KitConnection(
        val jdbcConnection: Connection,
        val driver: Driver = DefaultDriver) {

    public fun <T> select(sql: String, vararg arguments: Any?, mapper: (ResultSet) -> T): List<T> {
        return select(sql, *(arguments.map {Argument(it)}.toTypedArray())) { mapper(it) }
    }

    public fun <T> select(sql: String, vararg arguments: Argument, mapper: (ResultSet) -> T): List<T> {
        return preparedStatement(sql) {
            arguments.forEachIndexed { i, argument ->
                it.set(i + 1, prepareForSet(argument))
            }
            val result = arrayListOf<T>()
            val resultSet = it.executeQuery();
            while (resultSet.next()) {
                result.add(mapper(resultSet))
            }
            resultSet.close()

            result
        }
    }

    public fun insert(sql: String, vararg arguments: Any?): Unit = this.update(sql, *arguments)

    public fun insert(sql: String, vararg arguments: Argument): Unit = this.update(sql, *arguments)

    public fun <T> insert(sql: String, vararg arguments: Any?, f: (ResultSet) -> T): T {
        return this.insert(sql, *(arguments.map {Argument(it)}.toTypedArray())){ f(it) }
    }

    public fun <T> insert(sql: String, vararg arguments: Argument, f: (ResultSet) -> T): T {
        return preparedStatement(sql, defaultOptions().copy(returnGeneratedKeys = true)) {
            arguments.forEachIndexed { i, argument ->
                it.set(i + 1, prepareForSet(argument))
            }

            it.executeUpdate()

            val keysResultSet = it.generatedKeys
            try {
                require(keysResultSet.next()) { "No generated keys row was returned from statement" }
                f(keysResultSet)
            } finally {
                keysResultSet.close()
            }
        }
    }

    public fun update(sql: String, vararg arguments: Any?): Unit {
        this.update(sql, *(arguments.map {Argument(it)}.toTypedArray()))
    }

    public fun update(sql: String, vararg arguments: Argument): Unit {
        preparedStatement(sql) {
            arguments.asIterable().forEachIndexed { i, argument ->
                it.set(i + 1, prepareForSet(argument))
            }
            it.executeUpdate()
        }
    }

    public fun <T> preparedStatement(
            sql: String,
            options: StatementOptions = defaultOptions(),
            f: (PreparedStatement) -> T
    ) : T {
        val statement = this.jdbcConnection.prepareStatement(sql, when (options.returnGeneratedKeys){
            true -> Statement.RETURN_GENERATED_KEYS
            else -> Statement.NO_GENERATED_KEYS
        })
        try {
            return f(statement)
        }
        finally {
            statement.close()
        }
    }

    //

    private fun prepareForSet(argument: Argument): Any? = when (argument.hint) {
        null -> argument.value
        else -> {
            val serializer = driver.serializers.get(argument.hint)
            if (serializer != null) {
                serializer.serialize(argument.value)
            } else {
                argument.value
            }
        }
    }
}

data class StatementOptions(
        val returnGeneratedKeys: Boolean = false
)

public fun defaultOptions() : StatementOptions {
    return StatementOptions()
}

public open class Kit(
        val connectionProvider: () -> Connection,
        val driver: Driver = DefaultDriver
) {

    private var transaction : KitConnection? = null

    constructor(dataSource: DataSource, driver: Driver = DefaultDriver) : this(connectionProvider = {
        dataSource.connection
    }, driver = driver)

    private fun open():  Connection {
        val start = System.nanoTime()
        val connection = connectionProvider()
        val time = (System.nanoTime() - start) / 1000000L
        println("Connection opened in $time ms")
        return connection
    }

    public fun <R> connection(f: (KitConnection) -> R) :R {
        val connection = transaction ?: KitConnection(this.open(), driver = this.driver)
        val r = f(connection);
        if (transaction == null) {
            connection.jdbcConnection.close()
        }
        return r;
    }

    public fun transaction(f: (KitConnection) -> Unit) : Unit {
        this.connection { connection ->
            transaction = connection
            val autoCommitBefore = connection.jdbcConnection.autoCommit
            connection.jdbcConnection.autoCommit = false
            try {
                val result = f(connection)
                try {
                    connection.jdbcConnection.commit()
                }
                finally {
                    connection.jdbcConnection.autoCommit = autoCommitBefore
                }
            }
            catch (e: Exception) {
                connection.jdbcConnection.rollback()
            }
            finally {
                transaction = null
            }
        }
    }
}
