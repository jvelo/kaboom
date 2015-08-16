package khat.dao

import khat.dao.ConcreteReadDao
import khat.QueryBuilder
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
    val mapper: (ResultSet) -> M
    val tableName: String

    val filterWhere: List<String>
}

public open class Dao<M : Any, K>(ds: () -> DataSource, mapper: ((ResultSet) -> M)? = null) :
        ConcreteReadDao<M, K>(ds, mapper)

