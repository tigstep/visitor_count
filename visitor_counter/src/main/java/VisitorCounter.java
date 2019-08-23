import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.regex.Pattern;

public class VisitorCounter {
    static Logger LOGGER = Logger.getLogger(VisitorCounter.class);

    public static SparkSession init(){
        SparkConf sparkConf = new SparkConf()
                .setAppName("VisitorCounter");
        SparkSession session = SparkSession
                .builder()
                .config(sparkConf)
                .appName("VisitorCounter")
                .getOrCreate();
        return session;
    }

    public static void closeSession(SparkSession session){
        session.close();
    }

    public static StructField[] constructStructFeild(String struct){
        StructField[] structFields = new StructField[]{};
        if (struct == "input_data"){
            //Create a schema for the input_data
            structFields = new StructField[]{
                    new StructField("visitor_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("page_url", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("session_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("page_view_dt", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("test_variant_id", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("user_agent", DataTypes.StringType, true, Metadata.empty())
            };
        } else if(struct == "user_agent_map"){
            structFields = new StructField[]{
                    new StructField("user_agent", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("device_type", DataTypes.StringType, true, Metadata.empty())
            };
        }

        return structFields;
    }

    public static void question_1(SparkSession session, String inputDataFilePath, String outputLocation){
        //Create a structFeild for the input_data by calling constructStructFeild
        StructField[] inputDataStructFields = constructStructFeild("input_data");

        //Define schemas using the above structFeilds
        StructType schema = new StructType(inputDataStructFields);

        //Create a dataframe using the above created schema
        Dataset<Row> inputDataDF = session
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv(inputDataFilePath);

        //Create a temp view using the above originalDF
        inputDataDF.createOrReplaceTempView("input_data");

        //Define a SQL statement as per question's requirements
        String question_1_sql = "SELECT " +
                "COUNT(DISTINCT(visitor_id)) " +
                ", DATE_TRUNC('month', TO_DATE(page_view_dt)) " +
                "FROM input_data " +
                "GROUP BY DATE_TRUNC('month', TO_DATE(page_view_dt))";
        //Create a new DF as per question_1 requirements
        Dataset<Row> result = session.sql(question_1_sql);

        //Store to an output location
        result.write().csv(outputLocation);
        LOGGER.info("Successfully completed the question_1!");
    }

    public static void question_2(SparkSession session, String inputDataFilePath,  String outputLocation, String userAgentMapFilePath){
        //Create a structField for the input_data by calling constructStructFeild
        StructField[] inputDataStructFields = constructStructFeild("input_data");
        //Create a structFeild for the input_data by calling constructStructFeild
        StructField[] userAgentMapStructFeilds = constructStructFeild("user_agent_map");

        //Define schemas using the above structFeilds
        StructType inputDataSchema = new StructType(inputDataStructFields);
        StructType userAgentMapSchema = new StructType(userAgentMapStructFeilds);

        //Create a dataframe using the inputDataSchema
        Dataset<Row> inputDataDF = session
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(inputDataSchema)
                .csv(inputDataFilePath);
        //Create a dataframe using the userAgentMapSchema
        Dataset<Row> userAgentMapDF = session
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(userAgentMapSchema)
                .csv(userAgentMapFilePath);
        //Create a temp view using the inputDataDF
        inputDataDF.createOrReplaceTempView("input_data");
        //Create a temp view using the  userAgentMapDF
        userAgentMapDF.createOrReplaceTempView("user_agent_map");

        //Define a SQL statement as per question's requirements
        String question_2_sql = "SELECT COUNT(DISTINCT(inp.visitor_id)) " +
                ", DATE_TRUNC('month', TO_DATE(inp.page_view_dt)) " +
                ", usr.device_type " +
                "FROM input_data inp " +
                "JOIN user_agent_map usr " +
                "ON inp.user_agent = usr.user_agent " +
                "GROUP BY DATE_TRUNC('month', TO_DATE(inp.page_view_dt)), usr.device_type";
        //Create a new DF as per question_2 requirements
        Dataset<Row> result = session.sql(question_2_sql);

        //Store to an output location
        result.write().csv(outputLocation);
        LOGGER.info("Successfully completed the question_1!");
    }

    public static void question_3(SparkSession session, String inputDataFilePath, String outputLocation){
        //Create a schema for the input_data
        StructField[] inputDataStructFields = constructStructFeild("input_data");

        //Define schemas using the above structFeilds
        StructType inputDataSchema = new StructType(inputDataStructFields);

        //Create a dataframe using the inputDataSchema
        Dataset<Row> inputDataDF = session
                .read()
                .option("mode", "DROPMALFORMED")
                .schema(inputDataSchema)
                .csv(inputDataFilePath);

        //Create a temp view using the inputDataDF
        inputDataDF.createOrReplaceTempView("input_data");
        //Creating a new dataframe using spark sql
        //to check values in each column for SSN presence
        //and record in a separate PII_COLUMN
        String question_3_sql = "SELECT COUNT(DISTINCT(tmp.visitor_id)) " +
                ",tmp.exp " +
                "FROM " +
                "(SELECT visitor_id AS visitor_id " +
                ",EXPLODE(SPLIT(test_variant_id, '\\\\|')) AS exp " +
                "FROM input_data) AS tmp " +
                "GROUP BY tmp.exp";
        Dataset<Row> result = session.sql(question_3_sql);

        //Store to an output location
        result.write().csv(outputLocation);
        LOGGER.info("Successfully completed the question_1!");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            LOGGER.error("Please provide a source file, output location and log4j configuration file");
            System.exit(1);
        }
        String log4jConfigFile = args[2];
        PropertyConfigurator.configure(log4jConfigFile);
        SparkSession session = init();
        question_1(session, args[0], args[1] + "_solution_1");
        question_2(session, args[0], args[1] + "_solution_2", args[3]);
        question_3(session, args[0], args[1] + "_solution_3");
        closeSession(session);
    }
}
