package khat

import khat.reflection.findAnnotationInHierarchy
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import java.util.function.Supplier
import javax.sql.DataSource
import kotlin.properties.Delegates
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

// WIP
interface DaoInterface <M: Any, in K> {
    fun query(): QueryBuilder<M>

    fun withId(id: K): M?

    fun where(sql: String, vararg args: Any): List<M>

    fun count(): Long
    fun count(sql: String, vararg args: Any): Long

    fun insert(entity: M): Unit
    fun insertAndGet(entity: M): M?

    fun update(entity: M): Unit
}

suppress("UNCHECKED_CAST")
open class Dao<out M: Any, in K>(
    val dataSource: Supplier<DataSource>,
    val customMapper: ((ResultSet) -> M)? = null
) {

    val mapper: (ResultSet) -> M by Delegates.lazy {
        customMapper ?: DataClassConstructorMapper(getModelClass())
    }

    val tableName: String

    init {
        tableName = getModelClass().findAnnotationInHierarchy(javaClass<table>())?.let { it.name }
                ?: getModelClass().getSimpleName().toLowerCase()
    }

    fun query(): QueryBuilder<M> =
        QueryBuilder(dataSource, mapper, Query(select = "select * from ${tableName}", where = getFilterWhere()))

    fun count(): Long = query().select("select count(*) from ${tableName}").asCount()
    fun count(sql: String, vararg args: Any): Long {
        val query = query().select("select count(*) from ${tableName}").where(sql)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.asCount()
    }

    fun withId(id: K): M? = query().where("id = ?").argument(id).single()

    fun where(sql: String, vararg args: Any): List<M> {
        val query = query().where(sql)
        val bound = args.fold(query, { query, argument -> query.argument(argument) })
        return bound.execute()
    }

    // ---------------------------------------------------------------------------------------------

    private fun getParametrizedTypes(): Array<Type> {
        val types = (this.javaClass.getGenericSuperclass() as ParameterizedType).getActualTypeArguments()
        if (types.size() < 2) {
            throw IllegalStateException("Can't use a DAO without concrete types")
        }
        return types
    }

    private fun getModelType(): Type = getParametrizedTypes().get(0)
    private fun getModelClass(): Class<M> = getModelType() as Class<M>

    private fun getKeyType(): Type = getParametrizedTypes().get(1)

    private fun getFilterWhere() =
        getModelClass().findAnnotationInHierarchy(javaClass<filter>())?.let { listOf(it.where) } ?: listOf<String>()
}


