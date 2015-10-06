package kaboom.driver

import java.io.StringReader
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

open class Driver {
    val serializers = hashMapOf<String, ColumnTypeSerializer>()

    val typeDeserializers = hashMapOf<KClass<*>, TypeDeserializer>()
}

open class StandardDriver : Driver() {
    init {
        typeDeserializers.put(UUID::class, object : TypeDeserializer {
            override fun deserialize(value: Any): Any {
                return UUID.fromString(value.toString())
            }
        })

        typeDeserializers.put(JsonObject::class, object : TypeDeserializer {
            override fun deserialize(value: Any): Any {
                return Json.createReader(StringReader(value.toString())).read() as JsonObject
            }
        })
    }
}

public object DefaultDriver : StandardDriver()
