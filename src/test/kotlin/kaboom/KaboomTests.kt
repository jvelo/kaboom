package kaboom

import org.junit.Assert
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExternalResource
import org.junit.rules.TestWatcher
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.postgresql.ds.PGPoolingDataSource
import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target
import java.util.function.Supplier
import javax.sql.DataSource
import kotlin.platform.platformStatic

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
annotation class SqlBefore(val sql: String = "SELECT 1")

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
annotation class SqlAfter(val sql: String = "SELECT 1")

class SqlTest(dsp: () -> DataSource): TestWatcher() {

    val dataSource: DataSource

    init {
        dataSource = dsp()
    }

    override fun starting(description: Description?) {
        val sqlBefore = description?.getAnnotation(javaClass<SqlBefore>());
        if (sqlBefore != null) {
            val statement = dataSource.getConnection().createStatement()
            statement.execute(sqlBefore.sql)
        }
    }

    override fun finished(description: Description?) {
        val sqlAfter = description?.getAnnotation(javaClass<SqlAfter>());
        if (sqlAfter != null) {
            val statement = dataSource.getConnection().createStatement()
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
        sqlBefore = description?.getAnnotation(javaClass<SqlBefore>());
        sqlAfter = description?.getAnnotation(javaClass<SqlAfter>());
        return super.apply(statement, description)
    }

    override fun before() {
        if (sqlBefore != null) {
            val statement = dataSource.getConnection().createStatement()
            statement.execute(sqlBefore!!.sql)
        }
    }

    override fun after() {
        if (sqlAfter != null) {
            val statement = dataSource.getConnection().createStatement()
            statement.execute(sqlAfter!!.sql)
        }
    }
}

open class KhatTests {

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

        @ClassRule
        val classSql: SqlResource = SqlResource(dataSource)
    }
}