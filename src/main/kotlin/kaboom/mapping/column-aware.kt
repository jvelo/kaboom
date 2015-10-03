package kaboom.mapping

import kaboom.column
import kaboom.id
import kaboom.ignore
import java.lang.reflect.Constructor
import java.lang.reflect.Field
import kotlin.reflect.KClass

/**
 * Maps a SQL column to a field
 */
data class ColumnField(
        val fieldName: String,
        val fieldClass: KClass<*>,
        val columnName: String,
        val id: Boolean
)

interface FieldsColumnAware {
    val fields: List<ColumnField>

    val id: List<ColumnField>
}

@Suppress("UNCHECKED_CAST")
open class DataClassConstructorColumnAware<out M : Any>(val modelClass: KClass<out M>) : FieldsColumnAware {

    val constructor: Constructor<out M>
        get() {
            val constructors = modelClass.java.declaredConstructors

            if (constructors.size() < 1) {
                throw IllegalStateException("Could not find model constructor")
            }

            return constructors[0] as Constructor<M>
        }

    override val fields: List<ColumnField>
        get() {
            return modelClass.java.declaredFields
                    .filterNot { it.name.indexOf('$') >= 0 }
                    .filterNot { it.getDeclaredAnnotationsByType(ignore::class.java).size() > 0 }
                    .take(constructor.genericParameterTypes.size())
                    .mapIndexed { index, field ->
                        ColumnField(
                                field.name,
                                field.type.kotlin,
                                getAnnotatedColumnName(index) ?: field.name,
                                fieldIsId(field)
                        )
                    }
        }

    override val id: List<ColumnField>
        get() = this.fields.filter { it.id }

    private fun fieldIsId(field: Field) =
            field.name.equals("id", ignoreCase = false) || field.getDeclaredAnnotationsByType(id::class.java).size() > 0

    private fun getAnnotatedColumnName(index: Int): String? {
        val annotations = constructor.parameterAnnotations[index]
        return annotations?.singleOrNull {
            it?.annotationType()?.equals(column::class.java) ?: false
        }?.let { (it as column).name }
    }

}