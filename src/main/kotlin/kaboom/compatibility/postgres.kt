package kaboom.compatibility

import kaboom.dao.Dao
import kaboom.dao.ReadOnlyDao
import kaboom.db.ColumnTypeSerializer
import kaboom.db.DatabaseSupport
import kaboom.db.StandardDatabaseSupport
import java.sql.ResultSet
import javax.sql.DataSource

object PostgresDatabaseSupport : StandardDatabaseSupport() {
    init {
        serializers.putIfAbsent("jsonb", object : ColumnTypeSerializer {
            override fun serialize(value: Any?): Any {
                val jsonObject = org.postgresql.util.PGobject();
                jsonObject.setType("jsonb");
                jsonObject.setValue(value.toString());
                return jsonObject
            }
        })

        serializers.putIfAbsent("json", object : ColumnTypeSerializer {
            override fun serialize(value: Any?): Any {
                val jsonObject = org.postgresql.util.PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(value.toString());
                return jsonObject
            }
        })
    }
}

public open class PgReadOnlyDao<M : Any, K : Any>(dataSource: () -> DataSource,
                                                databaseSupport: DatabaseSupport = PostgresDatabaseSupport,
                                                mapper: ((ResultSet) -> M)? = null) :
        ReadOnlyDao<M, K>(dataSource, databaseSupport, mapper)

public open class PgDao<M : Any, K : Any>(dataSource: () -> DataSource,
                                        databaseSupport: DatabaseSupport = PostgresDatabaseSupport,
                                        mapper: ((ResultSet) -> M)? = null) :
        Dao<M, K>(dataSource, databaseSupport, mapper)
