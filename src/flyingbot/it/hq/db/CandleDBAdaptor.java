package flyingbot.it.hq.db;

import flyingbot.it.data.hq.Candle;
import flyingbot.it.util.Common;
import org.json.JSONObject;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Write candles to db
 */
public class CandleDBAdaptor implements Runnable {

	String URL, userName, password, databaseTable, connStr, insertSQL;

	Connection connDB = null;
	PreparedStatement statementDB = null;

	AtomicBoolean isStopped;

	// insert 500 candles each time
	public static long queryPerBatch = 500;
	// singleton
	static CandleDBAdaptor adaptorDB;
	/**
	 * candles waiting to be written.
	 */
	ConcurrentLinkedQueue<Candle> candleQueue;
	public static long reconnectMillis = 1000 * 60 * 60;
	// last access time to db
	long lastAccessDB = 0;
	static {
		try {
			adaptorDB = new CandleDBAdaptor();
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
	
	public static CandleDBAdaptor CreateSingleton() {
		return adaptorDB;
	}

	protected CandleDBAdaptor() throws Exception {
		isStopped = new AtomicBoolean(true);
		candleQueue = new ConcurrentLinkedQueue<Candle>();
		loadConfiguration();
	}

	@Override
	public void run() {
		Thread.currentThread().setName("Database deamon");
		
		isStopped.set(false);
		while (!isStopped.get()) {
			try {
				writeCandleDB(candleQueue);
				Thread.sleep(1000);
			} catch (Exception e) {
				Common.PrintException(e);
			}
		}
	}

	public void insertCandle(Candle c) {
		candleQueue.add(c);
	}

	public void stop() {
		isStopped.set(true);
	}

	public boolean isStopped() {
		return isStopped.get();
	}

	private void loadConfiguration() throws Exception {
		InputStream is = this.getClass().getResource("candledb_login.json").openStream();
		JSONObject obj = Common.LoadJSONObject(is);
		if (obj.has("URL") && obj.has("Username") && obj.has("Password")) {
			URL = obj.getString("URL");
			userName = obj.getString("Username");
			password = obj.getString("Password");
			databaseTable = obj.getString("Table");
			connStr = URL
					+ "?characterEncoding=utf8&useSSL=false"
					+ "&serverTimezone=UTC&rewriteBatchedStatements=true";
		}
		Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
	}
	
	private void connectDatabase() throws SQLException {
		// if it is too long from last access time
		long cur = System.currentTimeMillis();
		if (cur - lastAccessDB > reconnectMillis) {
			resetDatabase();
			lastAccessDB = cur;
			return;
		}
	}
	
	private void resetDatabase() throws SQLException {
		if (connDB == null || connDB.isClosed()) {
			initDatabase();
			return;
		}

		if (statementDB != null && !statementDB.isClosed())
		{
			statementDB.close();
		}
		if (connDB.isValid(1)) {
			connDB.close();
		}

		statementDB = null;
		connDB = null;
		initDatabase();
	}

	private void initDatabase() throws SQLException {
		connDB = DriverManager.getConnection(connStr, userName, password);

		if (connDB.getAutoCommit()) {
			connDB.setAutoCommit(false);
		}

		// form SQL
		insertSQL = "INSERT INTO `" + databaseTable + "` (`ProductID`,`InstrumentID`,`Period`,`SerialNo`,`JSON`) "
				+ "VALUES (?, ?, ?, ?, ?)";
		statementDB = connDB.prepareStatement(insertSQL);

	}
	
	private String getProductID(String instrumentID) {
		String ret = new String();
		if (instrumentID.length() > 1) {
			for (int i = 0; i < instrumentID.length(); ++i) {
				char c = instrumentID.charAt(i);
				if (Character.isAlphabetic(c)) {
					ret += c;
				}
				else {
					break;
				}
			}
		}
		return ret;
	}

	private void writeCandleDB(Queue<Candle> candles) throws Exception {
		long count = 0;
		if (candles.size() < 1) {
			return;
		}
		connectDatabase();
		
		while (candles.size() > 0) {
			Candle c = candles.poll();
			if (c == null) {
				continue;
			}

			// set params
			statementDB.setString(1, getProductID(c.InstrumentID));
			statementDB.setString(2, c.InstrumentID);
			statementDB.setInt(3, c.Period);
			statementDB.setString(4, c.SerialNo);
			statementDB.setString(5, c.ToJSON().toString(-1));
			statementDB.addBatch();

			// some inserts at a time
			if (++count % queryPerBatch == 0) {
				statementDB.executeBatch();
				connDB.commit();

				// reset
				count = 0;
			}
		}

		// insert remaining candles
		if (count > 0) {
			statementDB.executeBatch();
			connDB.commit();
			count = 0;
		}

		// update last access time
		lastAccessDB = System.currentTimeMillis();
	}

	@Override
	protected void finalize() {
		try {
			if (!connDB.isClosed()) {
				connDB.close();
			}
			if (!statementDB.isClosed()) {
				statementDB.close();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}
}
