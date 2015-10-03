package kaboom;

import java.util.List;
import org.junit.Test;

/**
 * @version $Id$
 */
public class JavaDaoTests extends KaboomTests
{

    private Persons persons = Persons.INSTANCE$;

    @Test
    public void testFromJava() {
        List<Person> persons =  this.persons.query().execute();
    }
}
