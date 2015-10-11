package kaboom

import kaboom.dao.Dao
import org.junit.Assert
import org.junit.Test
import java.util.*
import javax.json.Json
import javax.json.JsonObject
import javax.json.JsonString
import kotlin.properties.get
import kotlin.test.assertNotNull

data class Document(
        val id: UUID,

        @type("jsonb")
        val doc: JsonObject
)

@table("document")
data open public class Person(
        val id: UUID,

        @type("jsonb")
        val doc: JsonObject
) {
    val name: JsonString by doc
    val city: JsonString by doc
}

@table("document")
data open public class JSON(
        val id: UUID,

        @type("jsonb")
        @column("doc")
        val json: JsonObject
)

@filter("doc @> '{\"city\":\"Paris\"}'")
data public class Parisian(id: UUID, doc: JsonObject) : Person(id, doc) {}

object Persons : Dao<Persons, Person, UUID>(KaboomTests.kit)

object Parisians : Dao<Parisians, Parisian, UUID>(KaboomTests.kit)

object JsonPersons: Dao<JsonPersons, JSON, UUID>(KaboomTests.kit)

data class Planet(
        @generated val id: Int? = null,

        val name: String
)

object Planets : Dao<Planets, Planet, Int>(KaboomTests.kit)

@SqlBefore("""
    DROP TABLE IF EXISTS document;
    CREATE TABLE document (id uuid, doc jsonb);
    INSERT INTO document VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"name": "John", "city": "Paris"}');
    INSERT INTO document VALUES ('3c75d791-a654-421c-b442-768a4748dff4', '{"name": "Jane", "city": "Paris"}');
    INSERT INTO document VALUES ('efbdba38-d540-4a4c-924a-595b6b19ab7d', '{"name": "Georges", "city": "New-York"}');
    INSERT INTO document VALUES ('46b8e6c5-3637-4aa5-aaa6-c5d3809d1e52', '{"name": "Robert", "city": "New-York"}');

    DROP TABLE IF EXISTS planet;
    CREATE TABLE planet(id SERIAL, name VARCHAR);
""")
@SqlAfter("DROP TABLE document; DROP TABLE planet")
public class DaoTests : KaboomTests() {

    @Test
    fun test_insert_and_then_retrieve_with_serial_id() {
        Planets.insert(Planet(name = "Mars"))
        val mars = Planets.query().where("name = ?").argument("Mars").single()
        assertNotNull(mars)
        assertNotNull(mars?.id);
    }

    @Test
    fun test_insert_and_get_with_serial_id() {
        val mars = Planets.insertAndGet(Planet(name = "Mars"))
        assertNotNull(mars)
        assertNotNull(mars?.id);
    }

    @Test
    fun test_insert_jsonb() {
        Persons.insert(
                Person(
                        UUID.randomUUID(),
                        Json.createObjectBuilder()
                                .add("name", "Paul")
                                .add("city", "New-York")
                                .build()
                )
        )
    }

    // FIXME find out why this one hangs from time to time
    @Test
    fun test_update() {
        Assert.assertEquals(4, Persons.count())
        Assert.assertEquals(2, Parisians.count())

        Persons.update(Person(
                UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
                Json.createObjectBuilder()
                        .add("name", "Roger")
                        .add("city", "London")
                        .build()
        ))

        val updated = Persons.query().execute();
        val p = updated.first { it.id.equals(UUID.fromString("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11")) }
        Assert.assertEquals("Roger", p.name.string)
        Assert.assertEquals("London", p.city.string)
    }

    @Test
    fun test_using_named_column() {
        val jsonPersons = JsonPersons.query().execute();
        Assert.assertEquals(4, jsonPersons.size())
    }

    @Test
    fun test_transaction() {
        Planets.transaction {
            insert(Planet(name = "Mars"))
            val mars = query().where("name = ?").argument("Mars").single()
            Assert.assertNotNull(mars)
            query().where(",").single()
        }

        val mars = Planets.query().where("name = ?").argument("Mars").single()
        Assert.assertNull(mars)
    }

}