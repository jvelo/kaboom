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
    javaClass<Boolean>().isAssignableFrom(type) -> this.getBoolean(key)
    javaClass<Byte>().isAssignableFrom(type) -> this.getByte(key)
    javaClass<Short>().isAssignableFrom(type) -> this.getShort(key)
    javaClass<Int>().isAssignableFrom(type) -> this.getInt(key)
    javaClass<Long>().isAssignableFrom(type) -> this.getLong(key)
    javaClass<Float>().isAssignableFrom(type) -> this.getFloat(key)
    javaClass<Double>().isAssignableFrom(type) -> this.getDouble(key)
    javaClass<BigDecimal>().isAssignableFrom(type) -> this.getBigDecimal(key)
    javaClass<Timestamp>().isAssignableFrom(type) -> this.getTimestamp(key)
    javaClass<Time>().isAssignableFrom(type) -> this.getTime(key)
    javaClass<Date>().isAssignableFrom(type) -> this.getDate(key)
    javaClass<String>().isAssignableFrom(type) -> this.getString(key)
    // TODO enums
    //this.wasNull() -> null //(problem in sqlite)
    else -> this.get(key)
}

class DataClassConstructorMapper<out M : Any>(val modelClass: java.lang.Class<out M>) : (ResultSet) -> M {

    class ColumnField(
        val fieldName: String,
        val fieldClass: Class<*>,
        val columnName: String? = null
    )

    private val constructor: Constructor<M>
    private val fields: List<ColumnField>

    suppress("UNCHECKED_CAST")
    init {
        val constructors = modelClass.getDeclaredConstructors()

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructor = constructors[0] as Constructor<M>

        fields = modelClass.getDeclaredFields()
                .filterNot { it.getName().indexOf('$') >= 0 }
                .take(constructor.getGenericParameterTypes().size())
                .mapIndexed { index, field -> ColumnField(field.getName(), field.getType(), getColumnName(index)) }
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
                "Failed to invoke ${modelClass.getCanonicalName()} constructor.\nArguments:\n" +
                    args.map { it -> "- " + it?.javaClass?.getCanonicalName() + " : " + it }.joinToString("\n"), e)
        }
    }

    private fun getColumnName(index: Int): String? {
        val annotations = constructor.getParameterAnnotations()[index]
        return annotations?.singleOrNull {
            it?.annotationType()?.equals(javaClass<column>()) ?: false
        }?.let { (it as column).name }
    }

}