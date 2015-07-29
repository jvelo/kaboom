package khat

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.util.function.Supplier
import javax.sql.DataSource

open class Dao<M, in K>(val dataSource: Supplier<DataSource>) {

    val mapper: ResultSetMapper<M>
    val tableName: String

    suppress("UNCHECKED_CAST")
    init {
        tableName = getTableAnnotation()?.let { it.name } ?:
                this.javaClass.getSimpleName().toLowerCase()
        mapper = DataClassResultSetMapper<M>(getModelType() as java.lang.Class<M>)
    }

    fun query(): QueryBuilder<M> {
        return QueryBuilder(dataSource.get(), mapper,
                Query(select = "select * from ${tableName}", where = getFilterWhere())
        )
    }

    suppress("BASE_WITH_NULLABLE_UPPER_BOUND")
    fun withId(id: K): M? = query().where("id = ?").argument(id).single()

    fun findWhere(sql: String, vararg args: Any): List<M> {
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

    private fun getTableAnnotation() = (this.getModelType() as Class<M>)
            .getAnnotationsByType(javaClass<table>()).firstOrNull()

    private fun getModelType(): Type {
        return getParametrizedTypes().get(0)
    }

    private fun getKeyType(): Type {
        return getParametrizedTypes().get(0)
    }

    private fun getFilterWhere() = (this.getModelType() as Class<M>)
            .getAnnotationsByType(javaClass<filter>()).firstOrNull()?.let { listOf(it.where) } ?:
            listOf<String>()
}