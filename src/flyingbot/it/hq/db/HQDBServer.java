package flyingbot.it.hq.db;

import flyingbot.it.data.hq.Candle;
import flyingbot.it.data.log.SocketLoggerFactory;
import flyingbot.it.net.tcp.SocketDuplex;
import flyingbot.it.util.Common;
import flyingbot.it.util.Result;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class HQDBServer extends SocketDuplex {

	CandleDBAdaptor adaptorDB;

	private static Logger LOG;

	public static int DEFAULT_PORT = 9201;

	public HQDBServer(Socket Sock, CandleDBAdaptor Adaptor) {
		super(Sock);
		adaptorDB = Adaptor;
	}

    private static void InitLogger() {
        StackTraceElement[] traces = Thread.currentThread().getStackTrace();
        Class<?> clz = null;
        try {
            clz = Class.forName(traces[1].getClassName());
        } catch (ClassNotFoundException e) {
            Common.PrintException(e);
            return;
        }

        JSONObject obj = Common.LoadJSONObject(clz.getResourceAsStream("log_addr.json"));
        if (obj != null && obj.has("IP") && obj.has("Port")) {
            int port = 0;
            String ip = null;
            ip = obj.getString("IP");
            port = obj.getInt("Port");

            LOG = SocketLoggerFactory.GetInstance(clz.getCanonicalName(), ip, port);
        } else {
            Common.PrintException("Create logger failed.");
        }
	}

	@Override
	public void OnStream(byte[] Data) {
		try {
            String text = new String(Data, 0, Data.length, StandardCharsets.UTF_8);
			Candle c = Candle.Parse(new JSONObject(text));
			adaptorDB.insertCandle(c);
		} catch (Exception e) {
			Common.PrintException(e);
		}	
	}

	@Override
	public void OnDisconnect() {
		InetSocketAddress addr = (InetSocketAddress)this.GetSocketAddress();
		LOG.info("Client disconnected, " + addr.getHostString() + ":" + addr.getPort());
	}

	@Override
	public void OnHearbeatError(Result Reason) {
	}
	
	static Set<HQDBServer> servers;
	static ReentrantReadWriteLock lock;
	static {
		lock = new ReentrantReadWriteLock();
		servers = new HashSet<HQDBServer>();
	}
	
	public static void main(String[] args) {
		int port = DEFAULT_PORT;
		CandleDBAdaptor adaptor = null;

		InitLogger();

		try {
			StackTraceElement[] traces = Thread.currentThread().getStackTrace();

			InputStream is1 = Class.forName(traces[1].getClassName()).getResource("port.json").openStream();
			JSONObject ob = Common.LoadJSONObject(is1);
			if (ob.has("Port")) {
				port = ob.getInt("Port");
			}

			@SuppressWarnings("resource")
			ServerSocket ss = new ServerSocket(port);

            // create adaptor
			adaptor = CandleDBAdaptor.CreateSingleton();
			Common.GetSingletonExecSvc().execute(adaptor);

            // logging
			LOG.info("HQDB is listening on port: " + port);

			while (true) {
				Socket client = ss.accept();

                // validate client IP
				InetSocketAddress addr = (InetSocketAddress)client.getRemoteSocketAddress();
				String remoteIP = addr.getAddress().getHostAddress();

                InputStream is0 = null;
				try {
					is0 = Class.forName(traces[1].getClassName()).getResource("ip.json").openStream();
				}
				catch (IOException ex) {
					Common.PrintException(ex);
				}

                if (!Common.VerifyIP(remoteIP, is0)) {
                    LOG.info("Invalid client IP: " + remoteIP);
					client.close();
					continue;
				}

                // set Out-of-Band
				client.setOOBInline(false);

                // add server instance
				lock.writeLock().lock();
				servers.add(new HQDBServer(client, adaptor));
				lock.writeLock().unlock();

                // remove dead server instance
				Set<HQDBServer> tmp = new HashSet<HQDBServer>();
				lock.readLock().lock();
				for (HQDBServer s : servers) {
					if (!s.IsConnected()) {
						tmp.add(s);
					}
				}
				lock.readLock().unlock();
				lock.writeLock().lock();
				for (HQDBServer s : tmp) {
					servers.remove(s);
				}
				lock.writeLock().unlock();
			}
		} catch (Exception e) {
			Common.PrintException(e);
		}
	}

    @Override
    public void OnConnect() {
        InetSocketAddress addr = (InetSocketAddress) this.GetSocketAddress();
        LOG.info("Client connected, " + addr.getHostString() + ":" + addr.getPort());

        Thread.currentThread().setName("Client session (" + addr.getHostString() + ":" + addr.getPort() + ")");
    }
}
