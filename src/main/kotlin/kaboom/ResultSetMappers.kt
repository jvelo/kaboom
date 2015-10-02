package kaboom

import kaboom.types.Types
import java.lang.reflect.Constructor
import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp
import java.sql.Date
import kotlin.jdbc.get
import kotlin.reflect.KClass
import kotlin.reflect.jvm.java

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
class DataClassConstructorMapper<out M : Any>(val modelClass: java.lang.Class<out M>) : (ResultSet) -> M {

    class ColumnField(
        val fieldName: String,
        val fieldClass: Class<*>,
        val columnName: String? = null
    )

    private val constructor: Constructor<M>
    private val fields: List<ColumnField>

    init {
        val constructors = modelClass.declaredConstructors

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructor = constructors[0] as Constructor<M>

        fields = modelClass.declaredFields
                .filterNot { it.name.indexOf('$') >= 0 }
                .take(constructor.genericParameterTypes.size())
                .mapIndexed { index, field -> ColumnField(field.name, field.type, getColumnName(index)) }
    }

    override fun invoke(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName ?: it.fieldName, it.fieldClass)
            if (value != null) {
                Types.convert(it.fieldClass, value) ?: value
            } else {
                null
            }
        }.toTypedArray()
        try {
            return constructor.newInstance(*args)
        } catch(e: IllegalArgumentException) {
            throw RuntimeException(
                "Failed to invoke ${modelClass.canonicalName} constructor.\nArguments:\n" +
                    args.map { it -> "- " + it?.javaClass?.canonicalName + " : " + it }.joinToString("\n"), e)
        }
    }

    private fun getColumnName(index: Int): String? {
        val annotations = constructor.parameterAnnotations[index]
        return annotations?.singleOrNull {
            it?.annotationType()?.equals(column::class.java) ?: false
        }?.let { (it as column).name }
    }

}