package kaboom.dao

import kaboom.Query
import kaboom.QueryBuilder
import kaboom.db.DatabaseSupport
import kaboom.filter
import kaboom.jdbc.set
import kaboom.mapping.ColumnField
import kaboom.mapping.DataClassConstructorColumnAware
import kaboom.mapping.DataClassConstructorMapper
import kaboom.mapping.FieldsColumnAware
import kaboom.reflection.findAnnotationInHierarchy
import kaboom.table
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import javax.sql.DataSource
import kotlin.reflect.KClass

public open class ConcreteTableMappingAware<M : Any, K : Any>(
        override val dataSource: () -> DataSource,
        override val databaseSupport: DatabaseSupport,
        internal val customMapper: ((ResultSet) -> M)? = null
) : TableMappingAware<M, K> {

    override val tableName: String by lazy {
        this.javaClass.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.java.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.java.simpleName.toLowerCase()
    }

    override val mapper: (ResultSet) -> M by lazy {
        customMapper ?: DataClassConstructorMapper(modelClass)
    }

    @Suppress("UNCHECKED_CAST")
    val modelClass: KClass<M> by lazy {
        (parametrizedTypes.get(0) as Class<M>).kotlin
    }

    override val filterWhere: List<String> by lazy {
        this.javaClass.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: modelClass.java.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: listOf<String>()
    }

    val parametrizedTypes: Array<Type> by lazy {
        val types = (this.javaClass.genericSuperclass as ParameterizedType).actualTypeArguments
        if (types.size() < 2) {
            throw IllegalStateException("Can't use a DAO without concrete types")
        }
        types
    }
}

public open class ConcreteReadDao<M : Any, K : Any>(
        dataSource: () -> DataSource,
        databaseSupport: DatabaseSupport,
        mapper: ((ResultSet) -> M)? = null
) :
        ConcreteTableMappingAware<M, K>(dataSource, databaseSupport, mapper),
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

public open class ConcreteWriteDao<M : Any, K : Any>(
        dataSource: () -> DataSource,
        databaseSupport: DatabaseSupport,
        mapper: ((ResultSet) -> M)?
) :
        ConcreteReadDao<M, K>(dataSource, databaseSupport, mapper),
        ReadWriteDao<M, K> {

    val columnAware: FieldsColumnAware by lazy {
        DataClassConstructorColumnAware(modelClass, databaseSupport)
    }

    override fun update(entity: M) {
        val sqlBuilder = StringBuilder("UPDATE $tableName ")

        val updates = columnAware.fields.filterNot { it.id }.map {
            "set ${it.columnName} = ?"
        }
        sqlBuilder.append(updates.join(", "))
        val where = columnAware.id.map {
            "${it.columnName} = ?"
        }
        sqlBuilder.append(" WHERE ${where.join(" AND ")}")

        val statement = dataSource().connection.prepareStatement(sqlBuilder.toString())

        var index = 1
        columnAware.fields.filterNot { it.id }.forEach {
            statement.set(index, prepareForSet(it, fieldValue(entity, it)))
            index++
        }
        columnAware.id.forEach {
            statement.set(index, prepareForSet(it, fieldValue(entity, it)))
            index++
        }
        statement.executeUpdate()
    }

    private fun fieldValue(entity: M, field: ColumnField): Any? {
        val f = entity.javaClass.getDeclaredField(field.fieldName)
        f.isAccessible = true
        return f.get(entity)
    }

    private fun prepareForSet(field: ColumnField, value: Any?): Any? = when (field.typeHint) {
        null -> value
        else -> {
            val serializer = databaseSupport.serializers.get(field.typeHint)
            if (serializer != null) {
                serializer.serialize(value)
            } else {
                value
            }
        }
    }

    override fun insertAndGet(entity: M): M? {
        throw UnsupportedOperationException()
    }

    override fun insert(entity: M) {
        throw UnsupportedOperationException()
    }
}