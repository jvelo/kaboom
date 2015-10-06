package kaboom.mapping

import kaboom.driver.Driver
import kaboom.driver.DefaultDriver
import kaboom.jdbc.get
import java.sql.ResultSet
import kotlin.reflect.KClass


@Suppress("UNCHECKED_CAST")
class DataClassConstructorMapper<out M : Any>(modelClass: KClass<out M>, driver: Driver = DefaultDriver) :
        (ResultSet) -> M,
        DataClassConstructorColumnAware<M>(modelClass, driver) {

    override fun invoke(rs: ResultSet): M {
        val args = fields.map {
            val value = rs.get(it.columnName, it.fieldClass)
            when {
                value != null -> {
                    val deserializer = driver.typeDeserializers.get(it.fieldClass)
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