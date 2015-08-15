package khat.dao

import khat.dao.ConcreteReadDao
import khat.QueryBuilder
import java.sql.ResultSet
import javax.sql.DataSource

// WIP
public interface ReadDao <out M : Any, in K> {
    fun query(): QueryBuilder<M>

    fun withId(id: K): M?

    fun where(sql: String, vararg args: Any): List<M>

    fun count(): Long
    fun count(sqlWhere: String, vararg args: Any): Long

    companion object {
        public fun of<Model : Any, Key>(dataSource: DataSource): ReadDao<Model, Key>
                = ConcreteReadDao<Model, Key>({ dataSource })

        public fun of<Model : Any, Key>(dataSource: DataSource, mapper: (ResultSet) -> Model): ReadDao<Model, Key>
                = ConcreteReadDao<Model, Key>({ dataSource }, mapper)

        public fun of<Model : Any, Key>(dataSource: () -> DataSource): ReadDao<Model, Key>
                = ConcreteReadDao<Model, Key>(dataSource)

        public fun of<Model : Any, Key>(dataSource: () -> DataSource, mapper: (ResultSet) -> Model): ReadDao<Model, Key>
                = ConcreteReadDao<Model, Key>(dataSource, mapper)
    }
}

interface ReadWriteDao<M : Any, K> : ReadDao<M, K> {
    fun insert(entity: M): Unit
    fun insertAndGet(entity: M): M?

    fun update(entity: M): Unit
}

interface TableMappingAware<M : Any, in K> {
    val dataSource: () -> DataSource
    val mapper: (ResultSet) -> M
    val tableName: String

    fun getFilterWhere(): List<String>
}

public open class Dao<M : Any, K>(ds: () -> DataSource, mapper: ((ResultSet) -> M)? = null) :
        ConcreteReadDao<M, K>(ds,mapper)