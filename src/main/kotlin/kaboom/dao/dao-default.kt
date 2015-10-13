package kaboom.dao

import kaboom.*
import kaboom.jdbc.get
import kaboom.mapping.ColumnField
import kaboom.mapping.DataClassConstructorColumnAware
import kaboom.mapping.DataClassConstructorMapper
import kaboom.mapping.FieldsColumnAware
import kaboom.reflection.findAnnotationInHierarchy
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.sql.ResultSet
import kotlin.reflect.KClass

public open class ConcreteTableMappingAware<M : Any, K : Any>(
        override val kit: Kit,
        internal val customMapper: ((ResultSet) -> M)? = null
) : TableMappingAware<M, K> {

    override val tableName: String by lazy {
        this.javaClass.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.java.findAnnotationInHierarchy(table::class.java)?.let { it.name }
                ?: modelClass.java.simpleName.toLowerCase()
    }

    override val mapper: (ResultSet) -> M by lazy {
        customMapper ?: DataClassConstructorMapper(kit, modelClass)
    }

    @Suppress("UNCHECKED_CAST")
    val modelClass: KClass<M> by lazy {
        (parametrizedTypes.get(1) as Class<M>).kotlin
    }

    override val filterWhere: List<String> by lazy {
        this.javaClass.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: modelClass.java.findAnnotationInHierarchy(filter::class.java)?.let { listOf(it.where) }
                ?: listOf<String>()
    }

    val parametrizedTypes: Array<Type> by lazy {
        val types = (this.javaClass.genericSuperclass as ParameterizedType).actualTypeArguments
        if (types.size() < 3) {
            throw IllegalStateException("Can't use a DAO without concrete types")
        }
        types
    }
}

public open class ConcreteReadDao<Self : ConcreteReadDao<Self, M, K>, M : Any, K : Any>(
        kit: Kit,
        mapper: ((ResultSet) -> M)? = null
) :
        ConcreteTableMappingAware<M, K>(kit, mapper),
        ReadDao<Self, M, K> {

    override fun <R> transaction(f: Self.() -> R) : R {
        return kit.transaction {
            (this as Self).f()
        }
    }

    override fun query(): QueryBuilder<M> =
            QueryBuilder(kit, mapper, Query(select = "select * from $tableName", where = filterWhere))

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

public open class ConcreteWriteDao<Self: ConcreteWriteDao<Self, M, K>, M : Any, K : Any>(
        kit: Kit,
        mapper: ((ResultSet) -> M)?
) :
        ConcreteReadDao<Self, M, K>(kit, mapper),
        ReadWriteDao<Self, M, K> {

    val columns: FieldsColumnAware by lazy {
        DataClassConstructorColumnAware(modelClass)
    }

    override fun update(entity: M) {
        val sqlBuilder = StringBuilder("UPDATE $tableName ")

        val updates = columns.all.filterNot { it.id }.map {
            "set ${it.columnName} = ?"
        }
        sqlBuilder.append(updates.join(", "))
        val where = columns.id.map {
            "${it.columnName} = ?"
        }
        sqlBuilder.append(" WHERE ${where.join(" AND ")}")

        kit.connection { connection ->
            connection.update(sqlBuilder.toString(),
                    *columns.all.filterNot { it.id }.plus(columns.id).map{ Argument(fieldValue(entity, it), it.typeHint) }.toTypedArray())
        }
    }

    override fun insert(entity: M) {
        val insert = getInsert(entity)
        kit.connection { connection -> connection.insert(insert.first, *insert.second.toTypedArray()) }
    }

    override fun insertAndGet(entity: M): M? {
        val insert = getInsert(entity)
        return kit.connection { connection ->
            val keyElements = connection.insert(insert.first, *insert.second.toTypedArray()) { keys ->
                columns.id.map { keys.get(it.columnName, it.fieldClass) }
            }

            when (keyElements.size()) {
                0 -> null
                1 -> this.withId(keyElements.get(0) as K)
                else -> null
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    private fun getInsert(entity: M) : Pair<String, List<Argument>> {
        val fields = columns.all.filterNot { it.generated }
        return "INSERT INTO $tableName (${fields.map{it.columnName}.join(",")}) VALUES (${fields.map { "?" }.join(",")})" to fields.map {
            Argument(fieldValue(entity, it), it.typeHint)
        }
    }

    private fun fieldValue(entity: M, field: ColumnField): Any? {
        val f = entity.javaClass.getDeclaredField(field.fieldName)
        f.isAccessible = true
        return f.get(entity)
    }


}