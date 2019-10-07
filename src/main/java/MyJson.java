import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-10-07 16:26
 */
public class MyJson extends UDF {


    public String evaluate(String json, int index) {
        String[] fields = json.split("\"");

        return fields[4 * index - 1];
    }

}
