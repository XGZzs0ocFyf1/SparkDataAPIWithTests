
import com.dimafeng.testcontainers.{Container, ForAllTestContainer, PostgreSQLContainer}
import org.apache.spark.sql.test.SharedSparkSession


class ExampleTestContainer extends SharedSparkSession with ForAllTestContainer  {
  override val container: PostgreSQLContainer = PostgreSQLContainer()






}
