import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

public class EmbeddedPig {
	public static void main(String[] args) {
		try {
		 
		PigServer pigServer = new PigServer(ExecType.LOCAL);
		 
		runQuery(pigServer);
		 
		}catch(Exception e) {
		 
		e.printStackTrace();
		 
		}
		 
		}

public static void runQuery(PigServer pigServer) {
	 
	try {
	 
	pigServer.registerQuery("V = LOAD '/home/edureka/SAIWS/Dataset/PIG/Visits.csv' USING PigStorage(',') " +
			"AS (user:chararray,url:chararray,timestamp:chararray);");
	 
	pigServer.registerQuery("P = LOAD '/home/edureka/SAIWS/Dataset/PIG/Pages.csv' USING PigStorage(',')" +
			" AS (url:chararray,prnk:float);");
	 
	pigServer.registerQuery("J = JOIN V by url,P by url; ");
	 
	pigServer.registerQuery("F = Filter J by P::prnk > 0.5; ");
	
	pigServer.registerQuery("G = FOREACH F generate V::user, P::prnk;");
	
	pigServer.registerQuery("D = DISTINCT G;");
	
	 
	pigServer.registerQuery("STORE D INTO '/home/edureka/SAIWS/Dataset/PIG/TEMPSTORAGE' ;");
	 
	 
	} catch(Exception e) {
	 
	e.printStackTrace();
	 
	}
	 
	}
}
