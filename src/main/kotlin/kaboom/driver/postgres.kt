package kaboom.driver

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