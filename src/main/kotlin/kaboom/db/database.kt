package kaboom.db

import java.util.*
import javax.json.JsonObject
import javax.json.Json
import kotlin.reflect.KClass

interface ColumnTypeSerializer {
    fun serialize(value: Any?) : Any
}

interface TypeDeserializer {
    fun deserialize(value: Any) : Any
}

open class DatabaseSupport {
    val serializers = hashMapOf<String, ColumnTypeSerializer>()

    val typeDeserializers = hashMapOf<KClass<*>, TypeDeserializer>()
}

open class StandardDatabaseSupport : DatabaseSupport() {
    init {
        typeDeserializers.put(UUID::class, object : TypeDeserializer {
            override fun deserialize(value: Any): Any {
                return UUID.fromString(value.toString())
            }
        })

        typeDeserializers.put(JsonObject::class, object : TypeDeserializer {
            override fun deserialize(value: Any): Any {
                return Json.createReader(java.io.StringReader(value.toString())).read() as javax.json.JsonObject
            }
        })
    }
}

public object DefaultDatabaseSupport : StandardDatabaseSupport()
