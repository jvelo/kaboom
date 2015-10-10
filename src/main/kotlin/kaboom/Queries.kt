package kaboom

import org.slf4j.LoggerFactory
import java.sql.ResultSet
import javax.sql.DataSource

data class Query(
        val select: String,
        val where: List<String> = listOf(),
        val order: String? = null,
        val limit: Long? = null,
        val offset: Long? = null,
        val arguments: List<Any> = listOf()
)

class QueryBuilder<out M: Any>(
        val dataSource: () -> DataSource,
        val mapper: (ResultSet) -> M,
        val query: Query
) {

    val logger = LoggerFactory.getLogger("sql")

    fun select(select: String) =
            QueryBuilder<M>(dataSource, mapper, query.copy(select = select))

    fun where(where: String) =
            QueryBuilder<M>(dataSource, mapper, query.copy(where = query.where.plus(where)))

    fun limit(limit: Long) =
            QueryBuilder<M>(dataSource, mapper, query.copy(limit = limit))

    fun offset(offset: Long) =
            QueryBuilder<M>(dataSource, mapper, query.copy(offset = offset))

    fun order(order: String) =
            QueryBuilder<M>(dataSource, mapper, query.copy(order = order))

    fun argument(argument: Any) =
            QueryBuilder<M>(dataSource, mapper, query.copy(arguments = query.arguments.plus(argument)))

    fun arguments(vararg arguments: Any) =
            QueryBuilder<M>(dataSource, mapper, query.copy(arguments = query.arguments.plus(arguments)))

    fun execute(): List<M> = map(this.mapper)

    fun single(): M? = one(this.mapper)

    fun count(): Long = one { resultSet -> resultSet.getLong(1) }!!

    fun <T: Any> one(f: (ResultSet) -> T): T? {
         return withResultSet { rs ->
             if (!rs.next()) null else f(rs)
        }
    }

    fun <T: Any> map(f: (ResultSet) -> T) : List<T> {
        val result = arrayListOf<T>()
        withResultSet { rs ->
            while (rs.next()) {
                result.add(f(rs))
            }
        }
        return result
    }

    private fun <T: Any?> withResultSet(f: (ResultSet) -> T): T {
        val sql = serialize()
        val connection = dataSource().connection
        val statement = connection.prepareStatement(sql)
        query.arguments.forEachIndexed { i, argument ->
            statement.setObject(i + 1, argument)
        }
        logger.info(sql, query.arguments)
        val resultSet = statement.executeQuery();
        // Execute callback function
        val result = f(resultSet)

        // Close everything
        resultSet.close()
        statement.close()
        connection.close()

        return result
    }

    private fun serialize(): String = query.select +
            (if (query.where.size() > 0) (" WHERE " + this.query.where.join(" AND ")) else "") +
            (if (query.order != null) " ORDER BY ${query.order}" else "") +
            (if (query.limit != null) " LIMIT ${query.limit}" else "") +
            (if (query.offset != null) " LIMIT ${query.offset}" else "")
}