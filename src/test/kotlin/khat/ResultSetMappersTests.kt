package khat

import org.junit.Assert
import org.junit.Test

@SqlBefore("""
    DROP TABLE IF EXISTS documents;
    CREATE TABLE documents (id uuid, doc jsonb)
""")
@SqlAfter("DROP TABLE documents")
public class ResultSetMappersTests : KhatTests() {

    @SqlBefore("""
        INSERT INTO documents VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a": "b", "c": 2}');
        INSERT INTO documents VALUES ('3c75d791-a654-421c-b442-768a4748dff4', '{"toto":  "tata"}');
    """)
    @SqlAfter("TRUNCATE TABLE documents")
    Test fun testMapper() {
    }

}