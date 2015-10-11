package kaboom.driver

import kaboom.Kit
import kaboom.dao.Dao
import kaboom.dao.ReadOnlyDao
import java.sql.ResultSet
import javax.sql.DataSource

object PostgresDriver : StandardDriver() {
    init {
        serializers.putIfAbsent("jsonb", object : ColumnTypeSerializer {
            override fun serialize(value: Any?): Any {
                val jsonObject = org.postgresql.util.PGobject();
                jsonObject.type = "jsonb";
                jsonObject.value = value.toString();
                return jsonObject
            }
        })

        serializers.putIfAbsent("json", object : ColumnTypeSerializer {
            override fun serialize(value: Any?): Any {
                val jsonObject = org.postgresql.util.PGobject();
                jsonObject.type = "json";
                jsonObject.value = value.toString();
                return jsonObject
            }
        })
    }
}

public open class PgReadOnlyDao<M : Any, K : Any>(kit: Kit,
                                                mapper: ((ResultSet) -> M)? = null) :
        ReadOnlyDao<M, K>(Kit(kit.connectionProvider, driver = PostgresDriver), mapper)

public open class PgDao<M : Any, K : Any>(kit: Kit,
                                        mapper: ((ResultSet) -> M)? = null) :
        Dao<M, K>(Kit(kit.connectionProvider, driver = PostgresDriver), mapper)
