package kaboom.types

import java.io.StringReader
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import kotlin.reflect.KClass

@Suppress("BASE_WITH_NULLABLE_UPPER_BOUND", "UNCHECKED_CAST")
object Types {

    val typeMappers = hashMapOf<KClass<*>, (Any) -> Any>()

    public fun <T> convert(type: Class<T>, value: Any): T? {
        val mapper = typeMappers.get(type.javaClass.kotlin)
        return mapper?.invoke(value) as T
    }

    public fun <T> convert(type: KClass<*>, value: Any): T? {
        val mapper = typeMappers.get(type)
        return mapper?.invoke(value) as T
    }

    public inline fun <reified T> registerMapper(noinline handler: (value: Any) -> Any) {
        typeMappers.put(T::class, handler)
    }
}

public fun registerDefaultTypesMappers() {
    Types.registerMapper<UUID> { value: Any -> UUID.fromString(value.toString()) }

    Types.registerMapper<JsonObject> { value: Any ->
        Json.createReader(StringReader(value.toString())).read() as JsonObject
    }
}
