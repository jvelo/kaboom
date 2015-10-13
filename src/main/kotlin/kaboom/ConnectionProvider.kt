package kaboom

import java.sql.Connection
import javax.sql.DataSource

public interface ConnectionProvider : () -> Connection

public open class DefaultConnectionProvider(val dataSource: DataSource): ConnectionProvider {

    override fun invoke(): Connection {
        return dataSource.connection
    }

}