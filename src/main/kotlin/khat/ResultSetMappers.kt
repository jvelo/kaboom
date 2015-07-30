package khat

import khat.types.Types
import khat.types.registerDefaultTypesMappers
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
                .filterNot { it.getName().equals("\$kotlinClass") }
                .mapIndexed { index, field -> ColumnField(field.getName(), field.getType(), getColumnName(index)) }
    }

    override fun map(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName ?: it.fieldName)
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