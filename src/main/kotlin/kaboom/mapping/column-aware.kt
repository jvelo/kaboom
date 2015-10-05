package kaboom.mapping

import kaboom.column
import kaboom.db.DatabaseSupport
import kaboom.db.DefaultDatabaseSupport
import kaboom.id
import kaboom.ignore
import kaboom.type
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
        val id: Boolean,
        val typeHint: String?
)

interface FieldsColumnAware {
    val fields: List<ColumnField>

    val id: List<ColumnField>
}

@Suppress("UNCHECKED_CAST")
open class DataClassConstructorColumnAware<out M : Any>(
        val modelClass: KClass<out M>,
        val databaseSupport: DatabaseSupport = DefaultDatabaseSupport
) : FieldsColumnAware {

    val constructor: Constructor<out M> by lazy {
        val constructors = modelClass.java.declaredConstructors

        if (constructors.size() < 1) {
            throw IllegalStateException("Could not find model constructor")
        }

        constructors[0] as Constructor<M>
    }

    override val fields: List<ColumnField> by lazy {
        modelClass.java.declaredFields
                .filterNot { it.name.indexOf('$') >= 0 }
                .filterNot { it.getDeclaredAnnotationsByType(ignore::class.java).size() > 0 }
                .take(constructor.genericParameterTypes.size())
                .mapIndexed { index, field ->
                    ColumnField(
                            field.name,
                            field.type.kotlin,
                            getAnnotatedColumnName(index) ?: field.name,
                            fieldIsId(field),
                            getAnnotatedColumnType(index)
                    )
                }
    }

    override val id: List<ColumnField> by lazy {
        this.fields.filter { it.id }
    }

    private fun fieldIsId(field: Field) =
            field.name.equals("id", ignoreCase = false) || field.getDeclaredAnnotationsByType(id::class.java).size() > 0

    private fun getAnnotatedColumnName(index: Int): String? = letForParameterAnnotation<column, String>(index) { name }

    private fun getAnnotatedColumnType(index:Int): String? = letForParameterAnnotation<type, String>(index) { value }

    private inline fun <reified A : Annotation, R: Any> letForParameterAnnotation(index: Int, f : A.() -> R ) : R? {
        val annotations = constructor.parameterAnnotations[index]
        return annotations?.singleOrNull {
            it?.annotationType()?.equals(A::class.java) ?: false
        }?.let { (it as A).f() }
    }

}