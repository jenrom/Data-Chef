package de.areto.datachef.worker;

import de.areto.datachef.Setup;
import de.areto.datachef.model.worker.WorkerCargo;
import de.areto.test.TestUtility;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;

import static com.google.common.base.Preconditions.checkNotNull;
import static de.areto.test.TestUtility.getSinkFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

@Slf4j
@RunWith(Parameterized.class)
public class ApplicationTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                  {"/test_sink/test_country.sink", "MAPPING", false}
                , {"/test_sink/test_country.20161016.csv", "FILE", false}
                , {"/test_sink/test_country.sink", "MAPPING", true} // Duplicate mapping
                , {"/test_sink/test_subregion.sink", "MAPPING", false}
                , {"/test_sink/employees.sink", "MAPPING", false}
                , {"/test_sink/failure.sink", "MAPPING", true} // Syntax errors
                , {"/test_sink/test_data_domain.sink", "MAPPING", false} // Stage: Connection
                , {"/test_sink/test_group_sreg.sink", "MAPPING", false} // Stage: Insert,
                , {"/test_sink/test_country.20161016.csv", "FILE", true} // Duplicate data file
                , {"/test_sink/test_country.group1.20161017.csv", "FILE", false}
                , {"/test_sink/test_subregion.20161017.csv", "FILE", false}
                , {"/test_sink/test_subregion.20161018.csv", "FILE", false}
                , {"/test_sink/employees.csv", "FILE", false}
                , {"test_data_domain", "SQL", false}
                , {"test_group_sreg", "SQL", false}
                , {"test_group_sreg", "ROLLBACK", false}
                , {"employees", "ROLLBACK", false}
                , {"test_country", "ROLLBACK", false}
                , {"/test_sink/test_country.sink", "MAPPING", false} // Redeployment
                , {"/test_sink/employees.sink", "MAPPING", false} // Redeployment
                , {"/test_sink/test_country.20161016.csv", "FILE", false} // Redeployment
                , {"/test_sink/test_country.group1.20161017.csv", "FILE", false} // Redeployment
                , {"/test_sink/employees.csv", "FILE", false} // Redeployment

                , {"/test_sink/fact_sales.mart", "MART", false}
                , {"/test_sink/fact_sales.mart", "MART", true} // Mart Duplicate
                , {"fact_sales", "MARTROLLBACK", false} // Mart rollback
                , {"/test_sink/fact_sales.mart", "MART", false}
                , {"/test_sink/fact_sales_2.mart", "MART", false}
                , {"/test_sink/fact_sales_3.mart", "MART", false}
                , {"/test_sink/fact_sales_4.mart", "MART", false}
                , {"/test_sink/fact_sa les_5.mart", "MART", true}

        });
    }

    @Parameterized.Parameter
    public String taskString;

    @Parameterized.Parameter(1)
    public String mode;

    @Parameterized.Parameter(2)
    public boolean shouldFail;

    @BeforeClass
    public static void setup() throws Exception {
        Setup.setup();
    }

    @Test
    public void taskShouldBeExecuted() {
        checkNotNull(taskString);
        checkNotNull(mode);

        log.info("Mode: {}, Task: {}, Should fail: {}", mode, taskString, shouldFail);

        switch (mode) {
            case "MAPPING":
                try {
                    executeWorker(new MappingWorker(getSinkFileFromResource(taskString)));
                } catch (Exception e) {
                    fail("File should be accessible");
                }
                break;

            case "MART":
                try {
                    executeWorker(new MartWorker(getSinkFileFromResource(taskString)));
                } catch (IOException | URISyntaxException e) {
                    fail("File should be accessible");
                } catch (IllegalStateException e) {
                    if(!shouldFail)
                        fail(e.getMessage());
                }
                break;

            case "FILE":
                try {
                    executeWorker(new MappingDataFileWorker(getSinkFileFromResource(taskString)));
                } catch (Exception e) {
                    fail("File should be accessible");
                }
                break;

            case "SQL":
                executeWorker(new MappingDataSQLWorker(taskString));
                break;

            case "ROLLBACK":
                executeWorker(new RollbackMappingWorker(taskString));
                break;

            case "MARTROLLBACK":
                executeWorker(new RollbackMartWorker(taskString));
                break;

            default:
                fail("Mode unknown");
                break;
        }
    }

    private void executeWorker(Worker worker) {
        assertThat(worker).isNotNull();
        final WorkerCargo cargo = TestUtility.executeWorker(worker);
        log.info("Worker {} --> #{}", worker, worker.cargo.getDbId());
        assertThat(cargo).isNotNull();
        assertThat(cargo.getErrors()).isNotNull();
        final boolean failed = cargo.hasErrors();
        assertThat(failed).isEqualTo(shouldFail);
    }
}
