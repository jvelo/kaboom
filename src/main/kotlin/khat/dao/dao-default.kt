package khat.dao

import khat.*
import khat.dao.ReadDao
import khat.reflection.findAnnotationInHierarchy
import java.io.Serializable
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import java.util.function.Supplier
import javax.sql.DataSource
import kotlin.properties.Delegates
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

open class ConcreteTableMappingAware<M : Any, K>(
        override val dataSource: () -> DataSource,
        val customMapper: ((ResultSet) -> M)? = null
) : TableMappingAware<M, K> {

    override val tableName: String
        get() = getModelClass().findAnnotationInHierarchy(javaClass<table>())?.let { it.name }
                ?: getModelClass().getSimpleName().toLowerCase()

    override val mapper: (ResultSet) -> M by Delegates.lazy {
        customMapper ?: DataClassConstructorMapper(getModelClass())
    }

    override fun getFilterWhere() =
            getModelClass().findAnnotationInHierarchy(javaClass<filter>())?.let { listOf(it.where) } ?: listOf<String>()

    private fun getModelType(): Type = getParametrizedTypes().get(0)

    suppress("UNCHECKED_CAST")
    private fun getModelClass(): Class<M> = getModelType() as Class<M>

    private fun getKeyType(): Type = getParametrizedTypes().get(1)

    private fun getParametrizedTypes(): Array<Type> {
        val types = (this.javaClass.getGenericSuperclass() as ParameterizedType).getActualTypeArguments()
        if (types.size() < 2) {
            throw IllegalStateException("Can't use a DAO without concrete types")
        }
        return types
    }
}

open class ConcreteReadDao<M : Any, K>(dataSource: () -> DataSource,
                               mapper: ((ResultSet) -> M)? = null) :
        ConcreteTableMappingAware<M, K>(dataSource, mapper),
        ReadDao<M, K> {

    override fun query(): QueryBuilder<M> =
            QueryBuilder(dataSource, mapper, Query(select = "select * from ${tableName}", where = getFilterWhere()))

    override fun count(): Long = query().select("select count(*) from ${tableName}").asCount()
    override fun count(sqlWhere: String, vararg args: Any): Long {
        val query = query().select("select count(*) from ${tableName}").where(sqlWhere)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.asCount()
    }

    override fun withId(id: K): M? = query().where("id = ?").argument(id).single()

    override fun where(sql: String, vararg args: Any): List<M> {
        val query = query().where(sql)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.execute()
    }
}