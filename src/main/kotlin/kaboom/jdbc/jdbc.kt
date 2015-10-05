package kaboom.jdbc

import java.math.BigDecimal
import java.sql.*
import javax.json.JsonObject
import kotlin.jdbc.get
import kotlin.reflect.KClass

public fun PreparedStatement.set(index: Int, value: Any?): Unit = when (value) {
    null -> this.setObject(index, null)
    is Boolean -> this.setBoolean(index, value)
    is Byte -> this.setByte(index, value)
    is Short -> this.setShort(index, value)
    is Int -> this.setInt(index, value)
    is Long -> this.setLong(index, value)
    is Double -> this.setDouble(index, value)
    is Float -> this.setFloat(index, value)
    is BigDecimal -> this.setBigDecimal(index, value)
    is Timestamp -> this.setTimestamp(index, value)
    is Time -> this.setTime(index, value)
    is Date -> this.setDate(index, value)
    is String -> this.setString(index, value)
    is JsonObject -> this.setObject(index, value.toString())
    else -> this.setObject(index, value)
}

fun <T: Any> ResultSet.get(key: String, type: KClass<T>): Any? = when (type) {
    Boolean::class -> this.getBoolean(key)
    Byte::class -> this.getByte(key)
    Short::class -> this.getShort(key)
    Int::class -> this.getInt(key)
    Long::class -> this.getLong(key)
    Float::class-> this.getFloat(key)
    Double::class -> this.getDouble(key)
    BigDecimal::class -> this.getBigDecimal(key)
    Timestamp::class -> this.getTimestamp(key)
    Time::class -> this.getTime(key)
    Date::class -> this.getDate(key)
    String::class -> this.getString(key)
    // TODO enums
    //this.wasNull() -> null //(problem in sqlite)
    else -> this.get(key)
}
