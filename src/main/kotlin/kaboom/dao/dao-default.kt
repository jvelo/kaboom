package kaboom.dao

import kaboom.*
import kaboom.reflection.findAnnotationInHierarchy
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import javax.sql.DataSource

public open class ConcreteTableMappingAware<M : Any, K : Any>(
        override val dataSource: () -> DataSource,
        internal val customMapper: ((ResultSet) -> M)? = null
) : TableMappingAware<M, K> {

    override val tableName: String
        get() = this.javaClass.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.simpleName.toLowerCase()

    override val mapper: (ResultSet) -> M by lazy(LazyThreadSafetyMode.NONE) {
        customMapper ?: DataClassConstructorMapper(modelClass)
    }

    @Suppress("UNCHECKED_CAST")
    val modelClass: Class<M>
        get() = parametrizedTypes.get(0) as Class<M>

    override val filterWhere: List<String>
        get() = this.javaClass.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: modelClass.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: listOf<String>()

    val parametrizedTypes: Array<Type>
        get() {
            val types = (this.javaClass.genericSuperclass as ParameterizedType).actualTypeArguments
            if (types.size() < 2) {
                throw IllegalStateException("Can't use a DAO without concrete types")
            }
            return types
        }
}

public open class ConcreteReadDao<M : Any, K : Any>(
        dataSource: () -> DataSource,
        mapper: ((ResultSet) -> M)? = null
) :
        ConcreteTableMappingAware<M, K>(dataSource, mapper),
        ReadDao<M, K> {

    override fun query(): QueryBuilder<M> =
            QueryBuilder(dataSource, mapper, Query(select = "select * from $tableName", where = filterWhere))

    override fun count(): Long = query().select("select count(*) from $tableName").count()

    override fun count(sqlWhere: String, vararg args: Any): Long {
        val query = query() select "select count(*) from $tableName" where sqlWhere
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.count()
    }

    override fun withId(id: K): M? = query().where("id = ?").argument(id).single()

    override fun where(sql: String, vararg args: Any): List<M> {
        val query = query().where(sql)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.execute()
    }
}