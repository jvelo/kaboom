package kaboom

import kaboom.driver.PostgresDriver
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.ExternalResource
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.postgresql.ds.PGPoolingDataSource
import javax.sql.DataSource
import kotlin.annotation.*

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class SqlBefore(val sql: String = "SELECT 1")

@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
annotation class SqlAfter(val sql: String = "SELECT 1")

class SqlTest(dsp: () -> DataSource): TestWatcher() {

    val dataSource: DataSource

    init {
        dataSource = dsp()
    }

    override fun starting(description: Description?) {
        val sqlBefore = description?.getAnnotation(SqlBefore::class.java);
        if (sqlBefore != null) {
            val statement = dataSource.connection.createStatement()
            statement.execute(sqlBefore.sql)
        }
    }

    override fun finished(description: Description?) {
        val sqlAfter = description?.getAnnotation(SqlAfter::class.java);
        if (sqlAfter != null) {
            val statement = dataSource.connection.createStatement()
            statement.execute(sqlAfter.sql)
        }
    }
}

class SqlResource(dsp: () -> DataSource) : ExternalResource() {

    var sqlBefore: SqlBefore? = null
    var sqlAfter: SqlAfter? = null

    val dataSource: DataSource

    init {
        dataSource = dsp()
    }

    override fun apply(statement: Statement?, description: Description?): Statement? {
        sqlBefore = description?.getAnnotation(SqlBefore::class.java);
        sqlAfter = description?.getAnnotation(SqlAfter::class.java);
        return super.apply(statement, description)
    }

    override fun before() {
        if (sqlBefore != null) {
            val connection = dataSource.connection
            val statement = connection.createStatement()
            statement.execute(sqlBefore!!.sql)
            statement.close()
            connection.close()
        }
    }

    override fun after() {
        if (sqlAfter != null) {
            val connection = dataSource.connection
            val statement = connection.createStatement()
            statement.execute(sqlAfter!!.sql)
            statement.close()
            connection.close()
        }
    }
}

open class KaboomTests {

    @Rule
    public fun getSql(): SqlTest = _sql
    val _sql = SqlTest(Companion.dataSource)

    companion object {
        val dataSource = object: () -> DataSource {
            private val source: PGPoolingDataSource

            init {
                source = PGPoolingDataSource();
                this.initialize();
            }

            fun initialize() {
                source setDataSourceName "A Data Source"
                source setServerName "localhost"
                source setDatabaseName "khat_tests"
                source setUser "postgres"
                source setPassword ""
                source setMaxConnections 10
            }

            override fun invoke(): DataSource {
                return this.source
            }
        }
        val kit = Kit(
                dataSource = dataSource(),
                driver = PostgresDriver
        )

        @ClassRule
        val classSql: SqlResource = SqlResource(dataSource)
    }
}