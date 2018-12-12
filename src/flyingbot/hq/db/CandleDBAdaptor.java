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
 * �������ߴ洢�����ݿ�
 * 
 * @author �º���
 *
 */
public class CandleDBAdaptor implements Runnable {

	// ����Դ�ļ�JSON�ж�ȡ���ݿ��½��Ϣ
	String URL, userName, password, databaseTable, connStr, insertSQL;

	// ���ݿ��ִ�����
	Connection connDB = null;
	PreparedStatement statementDB = null;

	// ѭ���Ƿ��˳�
	AtomicBoolean isStopped;

	// ��д�����ݿ����־
	ConcurrentLinkedQueue<Candle> candleQueue;

	// ÿ��������־д���ݿ�һ�Σ���������ڴ�����
	public static long queryPerBatch = 500;
	
	// �ϴβ������ݿ�ĺ�����������һ��ʱ���Զ������������ݿ�
	long lastAccessDB = 0;
	public static long reconnectMillis = 1000 * 60 * 60;
	
	// ����
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
		// �����߳����ƣ�����ʶ��
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
		// ����ϴ����ݿ����ʱ�䣬������ܳ�ʱ���������ݿ�
		long cur = System.currentTimeMillis();
		if (cur - lastAccessDB > reconnectMillis) {
			resetDatabase();
			lastAccessDB = cur;
			return;
		}
	}
	
	private void resetDatabase() throws SQLException {
		// ���ݿ�δ��ʼ��
		if (connDB == null || connDB.isClosed()) {
			initDatabase();
			return;
		}
		
		// ��������
		if (statementDB != null && !statementDB.isClosed())
		{
			statementDB.close();
		}
		if (connDB.isValid(1)) {
			connDB.close();
		}
		
		// ǿ���ͷŵ��ϴ����ӵ����ã�ǿ��JVM GC
		statementDB = null;
		connDB = null;
		initDatabase();
	}

	private void initDatabase() throws SQLException {
		connDB = DriverManager.getConnection(connStr, userName, password);
		
		// ����������
		if (connDB.getAutoCommit()) {
			connDB.setAutoCommit(false);
		}
		
		// ׼���������
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
			
			// ׼������			
			statementDB.setString(1, getProductID(c.InstrumentID));
			statementDB.setString(2, c.InstrumentID);
			statementDB.setInt(3, c.Period);
			statementDB.setString(4, c.SerialNo);
			statementDB.setString(5, c.ToJSON().toString(-1));
			statementDB.addBatch();
			
			// ��������������ɣ����ύ�Ա����ڴ�������� 
			if (++count % queryPerBatch == 0) {
				statementDB.executeBatch();
				connDB.commit();
				
				// ��������
				count = 0;
			}
		}
		
		// ִ��ʣ�µ����
		if (count > 0) {
			statementDB.executeBatch();
			connDB.commit();
			count = 0;
		}
		
		// ���²������ݿ��ʱ��
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
