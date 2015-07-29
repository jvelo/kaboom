package khat

import java.io.StringReader
import java.lang.reflect.Constructor
import java.sql.ResultSet
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import kotlin.jdbc.get

interface ResultSetMapper<M> {
    fun map(rs: ResultSet): M
}

class DataClassResultSetMapper<M>(modelClass: java.lang.Class<M>)
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
                .filterNot { it.getName().equals("\$kotlinClass") }
                .mapIndexed { index, field -> ColumnField(field.getName(), field.getType(), getColumnName(index)) }

    }

    override fun map(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName ?: it.fieldName)
            val type = it.fieldClass
            when {
                type.isAssignableFrom(javaClass<JsonObject>()) -> {
                    val reader = Json.createReader(StringReader(value.toString()))
                    reader.read() as JsonObject
                }
                type.isAssignableFrom(javaClass<UUID>()) -> UUID.fromString(value.toString())
                else -> value
            }

        }.toTypedArray()
        return constructor.newInstance(*args)
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