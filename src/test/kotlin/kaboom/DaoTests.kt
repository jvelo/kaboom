package kaboom

import kaboom.dao.Dao
import kaboom.types.registerDefaultTypesMappers
import org.junit.Assert
import org.junit.Test
import java.util.UUID
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString
import kotlin.properties.get

data class Document(
        val id: UUID,
        val doc: JsonObject
)

@table("document")
data open public class Person(
        val id: UUID,
        val doc: JsonObject
) {
    val name: JsonString by doc
    val city: JsonString by doc
}

@filter("doc @> '{\"city\":\"Paris\"}'")
data public class Parisian(id: UUID, doc: JsonObject) : Person(id, doc) {}

@SqlBefore("""
    DROP TABLE IF EXISTS document;
    CREATE TABLE document (id uuid, doc jsonb);
    INSERT INTO document VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"name": "John", "city": "Paris"}');
    INSERT INTO document VALUES ('3c75d791-a654-421c-b442-768a4748dff4', '{"name": "Jane", "city": "Paris"}');
    INSERT INTO document VALUES ('efbdba38-d540-4a4c-924a-595b6b19ab7d', '{"name": "Georges", "city": "New-York"}');
    INSERT INTO document VALUES ('46b8e6c5-3637-4aa5-aaa6-c5d3809d1e52', '{"name": "Robert", "city": "New-York"}');
""")
@SqlAfter("DROP TABLE document")
public class DaoTests : KaboomTests() {

    companion object {
        object Persons : Dao<Person, UUID>(KaboomTests.dataSource)

        object Parisians : Dao<Parisian, UUID>(KaboomTests.dataSource)
    }

    @Test fun testMapper() {
        registerDefaultTypesMappers()

        Assert.assertEquals(4, Companion.Persons.count())
        Assert.assertEquals(2, Companion.Parisians.count())

        Persons.update(Person(
                UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                Json.createObjectBuilder()
                        .add("name", "Roger")
                        .add("city", "London")
                        .build()
        ))

        val updated = Companion.Persons.query().execute();
        val p = updated.first { it.id.equals(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")) }
        println (p.name)
        println (p.city)
    }

}