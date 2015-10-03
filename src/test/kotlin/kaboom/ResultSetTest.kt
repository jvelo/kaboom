package kaboom

import kaboom.mapping.get
import org.junit.Assert
import java.math.BigDecimal
import java.sql.ResultSet
import java.sql.Timestamp
import kotlin.jdbc.get
import org.junit.Test as test

public class ResultSetTests : KaboomTests() {

    @test fun test_get_boolean() {
        val rs = executeQuery("select TRUE as value")
        rs.next()
        Assert.assertTrue(rs.get("value", Boolean::class) is Boolean)
        Assert.assertTrue(rs.get("value") is Boolean)
    }

    @test fun test_get_big_decimal() {
        val rs = executeQuery("select 123456789.12345678::numeric as value")
        rs.next()
        Assert.assertTrue(rs.get("value", BigDecimal::class) is BigDecimal)
        Assert.assertTrue(rs.get("value") is BigDecimal)
    }

    @test fun test_get_timestamp() {
        val rs = executeQuery("select timestamp '1984-02-03' as value")
        rs.next()
        Assert.assertTrue(rs.get("value", Timestamp::class) is Timestamp)
        Assert.assertTrue(rs.get("value") is Timestamp)
    }

    private fun executeQuery(sql: String): ResultSet =
        KaboomTests.dataSource().connection.createStatement().executeQuery(sql)
}