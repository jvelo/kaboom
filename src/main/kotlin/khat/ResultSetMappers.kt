package khat

import khat.types.Types
import khat.types.registerDefaultTypesMappers
import java.io.StringReader
import java.lang.reflect.Constructor
import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.Time
import java.sql.Timestamp
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import kotlin.jdbc.get
import kotlin.reflect.KClass
import kotlin.reflect.jvm.internal.KClassImpl
import kotlin.reflect.jvm.java

interface ResultSetMapper<M> {
    fun map(rs: ResultSet): M
}

fun <T: Any> ResultSet.get(key: String, type: KClass<T>): Any? = this.get(key, type.java)

fun ResultSet.get(key: String, type: Class<*>): Any? = when {
    this.wasNull() -> null
    type.isAssignableFrom(javaClass<Boolean>()) -> this.getBoolean(key)
    type.isAssignableFrom(javaClass<Byte>()) -> this.getByte(key)
    type.isAssignableFrom(javaClass<Short>()) -> this.getShort(key)
    type.isAssignableFrom(javaClass<Int>()) -> this.getInt(key)
    type.isAssignableFrom(javaClass<Long>()) -> this.getLong(key)
    type.isAssignableFrom(javaClass<Float>()) -> this.getFloat(key)
    type.isAssignableFrom(javaClass<Double>()) -> this.getDouble(key)
    type.isAssignableFrom(javaClass<BigDecimal>()) -> this.getBigDecimal(key)
    type.isAssignableFrom(javaClass<Timestamp>()) -> this.getTimestamp(key)
    type.isAssignableFrom(javaClass<Time>()) -> this.getTime(key)
    type.isAssignableFrom(javaClass<Date>()) -> this.getDate(key)
    type.isAssignableFrom(javaClass<String>()) -> this.getString(key)
    else -> this.get(key)
}

class DataClassConstructorMapper<M>(val modelClass: java.lang.Class<M>)
: ResultSetMapper<M> {

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

    override fun map(rs: ResultSet): M {
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
        }?.let {
            (it as column).name
        }
    }

}