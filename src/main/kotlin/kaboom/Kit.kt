package kaboom

import kaboom.driver.DefaultDriver
import kaboom.driver.Driver
import java.sql.Connection
import javax.sql.DataSource

public open class Kit(
        val dataSource: DataSource,
        val connectionProvider: () -> Connection = DefaultConnectionProvider(dataSource),
        val driver: Driver = DefaultDriver
) {

    private var currentTransaction: KitConnection? = null

    private fun open():  Connection {
        val start = System.nanoTime()
        val connection = connectionProvider()
        val time = (System.nanoTime() - start) / 1000000L
        println("Connection opened in $time ms")
        return connection
    }

    public fun <R> connection(f: (KitConnection) -> R) :R {
        val connection = currentTransaction ?: KitConnection(this.open(), driver = this.driver)
        val r = f(connection);
        if (currentTransaction == null) {
            connection.jdbcConnection.close()
        }
        return r;
    }

    public fun <R> transaction(f: (KitConnection) -> R) : R {
        return this.connection { connection ->
            currentTransaction = connection
            try {
                connection.transaction {
                    f(connection)
                }
            }
            finally {
                currentTransaction = null
            }
        }
    }
}

class TransactionFailedException(message: String, throwable: Throwable): Exception(message, throwable)
