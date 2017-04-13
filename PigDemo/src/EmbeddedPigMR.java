import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public class EmbeddedPigMR extends Configured implements Tool {

	public static void main(String[] args) {
		try {
			ToolRunner.run(new EmbeddedPigMR(), args);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void runQuery(PigServer pigServer) {

		try {

			pigServer
					.registerQuery("V = LOAD '/user/edureka/Visits.csv' USING PigStorage(',') AS (user:chararray,url:chararray,timestamp:chararray);");

			pigServer
					.registerQuery("P = LOAD '/user/edureka/Pages.csv' USING PigStorage(',') AS (url:chararray,prnk:float);");

			pigServer.registerQuery("J = JOIN V by url,P by url; ");

			pigServer.registerQuery("F = Filter J by P::prnk > 0.5; ");

			pigServer
					.registerQuery("STORE F INTO '/user/edureka/TEMPSTORAGE' ;");

		} catch (Exception e) {

			e.printStackTrace();

		}

	}

	@Override
	public int run(String[] arg0) throws Exception {

		try {

			Configuration conf = new Configuration(Boolean.TRUE);
			conf.set("fs.defaultFS", "hdfs://localhost:8020");
			conf.set("fs.default.name", "hdfs://localhost:8020");
			PigServer pigServer = new PigServer(ExecType.MAPREDUCE, conf);

			runQuery(pigServer);

			// Properties props = new Properties();

			// props.setProperty("fs.default.name", "hdfs://localhost:9000");

		} catch (Exception e) {

			e.printStackTrace();

		}

		return 0;
	}
}
