package kaboom.dao

import kaboom.QueryBuilder
import kaboom.driver.Driver
import kaboom.driver.DefaultDriver
import java.sql.ResultSet
import javax.sql.DataSource

public interface ReadDao <out M : Any, in K> {
    fun query(): QueryBuilder<M>

    fun withId(id: K): M?

    fun where(sql: String, vararg args: Any): List<M>

    fun count(): Long
    fun count(sqlWhere: String, vararg args: Any): Long
}

interface ReadWriteDao<M : Any, in K> : ReadDao<M, K> {
    fun insert(entity: M): Unit
    fun insertAndGet(entity: M): M?

    fun update(entity: M): Unit
}

interface TableMappingAware<out M : Any, in K> {
    val dataSource: () -> DataSource
    val driver: Driver
    val mapper: (ResultSet) -> M
    val tableName: String

    val filterWhere: List<String>
}

public open class ReadOnlyDao<M : Any, K : Any>(dataSource: () -> DataSource,
                                                driver: Driver = DefaultDriver,
                                                mapper: ((ResultSet) -> M)? = null) :
        ConcreteReadDao<M, K>(dataSource, driver, mapper)

public open class Dao<M : Any, K : Any>(dataSource: () -> DataSource,
                                        driver: Driver = DefaultDriver,
                                        mapper: ((ResultSet) -> M)? = null) :
        ConcreteWriteDao<M, K>(dataSource, driver, mapper)
