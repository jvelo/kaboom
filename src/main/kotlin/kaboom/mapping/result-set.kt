package kaboom.mapping

import kaboom.types.Types
import java.math.BigDecimal
import java.sql.Date
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp
import kotlin.jdbc.get
import kotlin.reflect.KClass

fun <T: Any> ResultSet.get(key: String, type: KClass<T>): Any? = this.get(key, type.java)

fun ResultSet.get(key: String, type: Class<*>): Any? = when {
    Boolean::class.java.isAssignableFrom(type) -> this.getBoolean(key)
    Byte::class.java.isAssignableFrom(type) -> this.getByte(key)
    Short::class.java.isAssignableFrom(type) -> this.getShort(key)
    Int::class.java.isAssignableFrom(type) -> this.getInt(key)
    Long::class.java.isAssignableFrom(type) -> this.getLong(key)
    Float::class.java.isAssignableFrom(type) -> this.getFloat(key)
    Double::class.java.isAssignableFrom(type) -> this.getDouble(key)
    BigDecimal::class.java.isAssignableFrom(type) -> this.getBigDecimal(key)
    Timestamp::class.java.isAssignableFrom(type) -> this.getTimestamp(key)
    Time::class.java.isAssignableFrom(type) -> this.getTime(key)
    Date::class.java.isAssignableFrom(type) -> this.getDate(key)
    String::class.java.isAssignableFrom(type) -> this.getString(key)
    // TODO enums
    //this.wasNull() -> null //(problem in sqlite)
    else -> this.get(key)
}

@Suppress("UNCHECKED_CAST")
class DataClassConstructorMapper<out M : Any>(modelClass: KClass<out M>) :
        (ResultSet) -> M,
        DataClassConstructorColumnAware<M>(modelClass) {

    override fun invoke(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName, it.fieldClass)
            when {
                value != null -> Types.convert(it.fieldClass, value) ?: value
                else -> null
            }
        }.toTypedArray()
        try {
            return constructor.newInstance(*args)
        } catch(e: IllegalArgumentException) {
            throw RuntimeException(
                "Failed to invoke ${modelClass.java.canonicalName} constructor.\nArguments:\n" +
                    args.map { it -> "- " + it?.javaClass?.canonicalName + " : " + it }.joinToString("\n"), e)
        }
    }

}