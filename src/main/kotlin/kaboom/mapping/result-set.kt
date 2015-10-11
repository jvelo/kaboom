package kaboom.mapping

import kaboom.Kit
import kaboom.driver.Driver
import kaboom.driver.DefaultDriver
import kaboom.jdbc.get
import java.sql.ResultSet
import kotlin.reflect.KClass


@Suppress("UNCHECKED_CAST")
class DataClassConstructorMapper<out M : Any>(val kit: Kit, modelClass: KClass<out M>) :
        (ResultSet) -> M,
        DataClassConstructorColumnAware<M>(modelClass) {

    override fun invoke(rs: ResultSet): M {
        val args = all.map {
            val value = rs.get(it.columnName, it.fieldClass)
            when {
                value != null -> {
                    val deserializer = kit.driver.typeDeserializers.get(it.fieldClass)
                    deserializer?.deserialize(value) ?: value
                }
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