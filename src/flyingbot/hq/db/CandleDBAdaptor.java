package flyingbot.hq.db;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json.JSONObject;

import dmkp.common.util.Common;
import flyingbot.data.hq.Candle;

/**
 * 把蜡烛线存储到数据库
 * 
 * @author 陈宏葆
 *
 */
public class CandleDBAdaptor implements Runnable {

	// 从资源文件JSON中读取数据库登陆信息
	String URL, userName, password, databaseTable, connStr, insertSQL;

	// 数据库和执行语句
	Connection connDB = null;
	PreparedStatement statementDB = null;

	// 循环是否退出
	AtomicBoolean isStopped;

	// 待写入数据库的日志
	ConcurrentLinkedQueue<Candle> candleQueue;

	// 每隔若干日志写数据库一次，避免过大内存消耗
	public static long queryPerBatch = 500;
	
	// 上次操作数据库的毫秒数，超过一定时间自动重新连接数据库
	long lastAccessDB = 0;
	public static long reconnectMillis = 1000 * 60 * 60;
	
	// 单件
	static CandleDBAdaptor adaptorDB;
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
		// 设置线程名称，便于识别
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
		// 检查上次数据库操作时间，如果可能超时则重连数据库
		long cur = System.currentTimeMillis();
		if (cur - lastAccessDB > reconnectMillis) {
			resetDatabase();
			lastAccessDB = cur;
			return;
		}
	}
	
	private void resetDatabase() throws SQLException {
		// 数据库未初始化
		if (connDB == null || connDB.isClosed()) {
			initDatabase();
			return;
		}
		
		// 重置连接
		if (statementDB != null && !statementDB.isClosed())
		{
			statementDB.close();
		}
		if (connDB.isValid(1)) {
			connDB.close();
		}
		
		// 强行释放掉上次连接地引用，强迫JVM GC
		statementDB = null;
		connDB = null;
		initDatabase();
	}

	private void initDatabase() throws SQLException {
		connDB = DriverManager.getConnection(connStr, userName, password);
		
		// 设置事务处理
		if (connDB.getAutoCommit()) {
			connDB.setAutoCommit(false);
		}
		
		// 准备插入语句
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
			
			// 准备数据			
			statementDB.setString(1, getProductID(c.InstrumentID));
			statementDB.setString(2, c.InstrumentID);
			statementDB.setInt(3, c.Period);
			statementDB.setString(4, c.SerialNo);
			statementDB.setString(5, c.ToJSON().toString(-1));
			statementDB.addBatch();
			
			// 如果数量超过若干，则提交以避免内存过度消耗 
			if (++count % queryPerBatch == 0) {
				statementDB.executeBatch();
				connDB.commit();
				
				// 计数清零
				count = 0;
			}
		}
		
		// 执行剩下的语句
		if (count > 0) {
			statementDB.executeBatch();
			connDB.commit();
			count = 0;
		}
		
		// 更新操作数据库的时间
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
