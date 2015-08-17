package kaboom.types

import java.io.StringReader
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.jvm.internal.KClassImpl

suppress("BASE_WITH_NULLABLE_UPPER_BOUND", "UNCHECKED_CAST")
object Types {

    val typeMappers = hashMapOf<KClass<*>, (Any) -> Any>()

    public fun <T> convert(type: Class<T>, value: Any): T? {
        val mapper = typeMappers.get(KClassImpl(type))
        return mapper?.invoke(value) as T
    }

    public fun <T> convert(type: KClass<T>, value: Any): T? {
        val mapper = typeMappers.get(type)
        return mapper?.invoke(value) as T
    }

    public inline fun <reified T> registerMapper(noinline handler: (value: Any) -> T) {
        typeMappers.put(KClassImpl(javaClass<T>()), handler)
    }
}

public fun registerDefaultTypesMappers() {
    Types.registerMapper { value: Any -> UUID.fromString(value.toString()) }

    Types.registerMapper { value: Any ->
        Json.createReader(StringReader(value.toString())).read() as JsonObject
    }
}
