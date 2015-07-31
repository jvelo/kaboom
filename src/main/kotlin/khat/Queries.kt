package khat

import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.function.Supplier
import javax.sql.DataSource

data class Query(
        val select: String,
        val where: List<String> = listOf(),
        val order: String? = null,
        val limit: Long? = null,
        val offset: Long? = null,
        val arguments: List<Any> = listOf()
)

class QueryBuilder<M>(val dataSource: Supplier<DataSource>, val mapper: ResultSetMapper<M>, val query: Query) {

    val logger = LoggerFactory.getLogger("sql")

    fun where(where: String) = QueryBuilder<M>(dataSource, mapper,
            query.copy(where = query.where.plus(where)))

    fun limit(limit: Long) = QueryBuilder<M>(dataSource, mapper, query.copy(limit = limit))

    fun offset(offset: Long) = QueryBuilder<M>(dataSource, mapper, query.copy(offset = offset))

    fun order(order: String) = QueryBuilder<M>(dataSource, mapper, query.copy(order = order))

    fun argument(argument: Any) = QueryBuilder<M>(dataSource, mapper,
            query.copy(arguments = query.arguments.plus(argument)))

    fun arguments(vararg arguments: Any) = QueryBuilder<M>(dataSource, mapper,
            query.copy(arguments = query.arguments.plus(arguments)))

    fun execute(): List<M> {
        val rs = getResultSet()
        val result = arrayListOf<M>()
        while (rs.next()) {
            result.add(this.mapper.map(rs))
        }
        return result
    }

    suppress("BASE_WITH_NULLABLE_UPPER_BOUND")
    fun single(): M? {
        val rs = getResultSet()
        if (!rs.next()) {
            return null
        } else {
            return this.mapper.map(rs)
        }
    }

    private fun getResultSet(): ResultSet {
        val sql = serialize()
        logger.info(sql)
        val connection = dataSource.get().getConnection()
        val statement = connection.prepareStatement(sql)
        query.arguments.forEachIndexed { i, argument ->
            statement.setObject(i + 1, argument)
        }
        val rs = statement.executeQuery();
        return rs
    }

    private fun serialize(): String = query.select +
            (if (query.where.size() > 0) (" WHERE " + this.query.where.join(" AND ")) else "") +
            (if (query.order != null) " ORDER BY ${query.order}" else "") +
            (if (query.limit != null) " LIMIT ${query.limit}" else "") +
            (if (query.offset != null) " LIMIT ${query.offset}" else "")
}