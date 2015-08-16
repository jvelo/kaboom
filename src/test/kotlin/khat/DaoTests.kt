package khat

import khat.dao.Dao
import khat.dao.ReadDao
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.*
import javax.json.JsonObject
import javax.json.JsonString
import kotlin.properties.Delegates

@data class Document(
        val id: UUID,
        val doc: JsonObject
)

@table("document")
@data open public class Person(
        val id: UUID,
        val doc: JsonObject
) {
    val name: JsonString by Delegates.mapVal(doc)
}

@filter("doc @> '{\"city\":\"Paris\"}'")
@data public class Parisian(id: UUID, doc: JsonObject) : Person(id, doc) {}

@SqlBefore("""
    DROP TABLE IF EXISTS document;
    CREATE TABLE document (id uuid, doc jsonb);
    INSERT INTO document VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"name": "John", "city": "Paris"}');
    INSERT INTO document VALUES ('3c75d791-a654-421c-b442-768a4748dff4', '{"name": "Jane", "city": "Paris"}');
    INSERT INTO document VALUES ('efbdba38-d540-4a4c-924a-595b6b19ab7d', '{"name": "Georges", "city": "New-York"}');
    INSERT INTO document VALUES ('46b8e6c5-3637-4aa5-aaa6-c5d3809d1e52', '{"name": "Robert", "city": "New-York"}');
""")
@SqlAfter("DROP TABLE document")
public class DaoTests : KhatTests() {

    companion object {
        object Persons : Dao<Person, UUID>(KhatTests.dataSource)
        object Parisians : Dao<Parisian, UUID>(KhatTests.dataSource)
    }

    @Test fun testMapper() {
        Assert.assertEquals(4, Companion.Persons.count())
        Assert.assertEquals(2, Companion.Parisians.count())
    }

}