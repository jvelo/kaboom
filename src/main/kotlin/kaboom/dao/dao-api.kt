package kaboom.dao

import kaboom.Kit
import kaboom.QueryBuilder
import kaboom.driver.Driver
import kaboom.driver.DefaultDriver
import java.sql.ResultSet
import javax.sql.DataSource

public interface ReadDao <Self :ReadDao<Self, M, K>, out M : Any, in K> {
    fun query(): QueryBuilder<M>

    fun withId(id: K): M?

    fun where(sql: String, vararg args: Any): List<M>

    fun count(): Long
    fun count(sqlWhere: String, vararg args: Any): Long

    fun <R> transaction(f: Self.() -> R) : R
}

interface ReadWriteDao<Self : ReadWriteDao<Self, M, K>, M : Any, in K> : ReadDao<Self, M, K> {
    fun insert(entity: M): Unit
    fun insertAndGet(entity: M): M?

    fun update(entity: M): Unit
}

interface TableMappingAware<out M : Any, in K> {
    val kit: Kit
    val mapper: (ResultSet) -> M
    val tableName: String

    val filterWhere: List<String>
}

public open class ReadOnlyDao<Self : ReadOnlyDao<Self, M, K>, M : Any, K : Any>(kit: Kit,
                                                mapper: ((ResultSet) -> M)? = null) :
        ConcreteReadDao<Self, M, K>(kit, mapper)

public open class Dao<Self: Dao<Self, M, K>, M : Any, K : Any>(kit: Kit,
                                        mapper: ((ResultSet) -> M)? = null) :
        ConcreteWriteDao<Self, M, K>(kit, mapper)
