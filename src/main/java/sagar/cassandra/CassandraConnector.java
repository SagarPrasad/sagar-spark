package sagar.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraConnector {
	private static Cluster cluster;
	private static Session session;
	static CassandraConnector client = new CassandraConnector();

	public void test() {
		session = client.connect("127.0.0.1");
		String cqlStatement = "SELECT * FROM test";
		for (Row row : session.execute(cqlStatement)) {
			System.out.println(row.toString());
		}
		// client.close();
	}

	public static void persist(String movieid, Integer count) {

		try {
			if (null == session) {
				session = client.connect("127.0.0.1");
			}
			
			PreparedStatement ps = session.prepare("INSERT INTO top_movie (movieid, viewscnt, time) VALUES (?, ?, dateof(now()))");
			//System.out.println("Query String ::: " + ps.getQueryString());
			/*Statement statement = new SimpleStatement(
					"INSERT INTO top_movie (movieid, viewscnt, time) VALUES (?, ?, dateof(now()))",
					movieId, count);*/
			long l = count;
			Long viewscnt = new Long(l);
			
			BoundStatement bind = ps.bind(movieid, viewscnt);
			//System.out.println("Preapred Statement " + bind.preparedStatement().getQueryString());
			
			session.execute(bind);
			
			System.out.println("Inserted the data for " + movieid);

		} catch (Exception e) {
			System.out.println(" Error while persisting the data in cassandra "
					+ e);
			e.printStackTrace();
		}
	}

	private void close() {
		cluster.close();
	}

	private Session connect(String node) {
		cluster = Cluster.builder().addContactPoint(node).build();
		Metadata metadata = cluster.getMetadata();
		System.out.printf("Connected to cluster: %s\n",
				metadata.getClusterName());
		for (Host host : metadata.getAllHosts()) {
			System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
					host.getDatacenter(), host.getAddress(), host.getRack());
		}
		return cluster.connect("test");
	}

}
