package sparktest.example;

import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.spark_project.jetty.http.HttpStatus;

@Deprecated
public class RestLogger extends Logger {

	public RestLogger(final Logger target) {
		super(target.getName());
		this.shadowedLogger = target;
	}

	private Logger shadowedLogger;

	private HttpClient httpClient = HttpClients.createDefault();

	private void log(final Object msg) {
		HttpPost postRequest = new HttpPost("http://log-catcher:8080/logCatcher/log");
		List<NameValuePair> params = new LinkedList<>();

		params.add(new BasicNameValuePair("identifier", System.getenv("HOSTNAME")));
		params.add(new BasicNameValuePair("message", msg.toString()));

		postRequest.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
		HttpResponse response;
		try {
			response = this.httpClient.execute(postRequest);
		} catch (Exception e) {
			this.shadowedLogger.error("Failed to send message to log-catcher instance.", e);
			return;
		}
		int status = response.getStatusLine().getStatusCode();
		if (status >= HttpStatus.OK_200 && status < HttpStatus.MULTIPLE_CHOICES_300) {
			return; // yay!
		}
		if (status < HttpStatus.OK_200 || status >= HttpStatus.INTERNAL_SERVER_ERROR_500) {
			this.shadowedLogger
					.error("There was an internal server error while reporting (Status code " + status + ")");
		}
		this.shadowedLogger.error("There was a user error while reporting (Status code " + status + ")");

	}

	private void log(final Object msg, final Throwable e) {
		this.log(msg + "\n" + e.getLocalizedMessage());
	}

	@Override
	public void debug(final Object message) {
		this.shadowedLogger.debug(message);
		this.log("[debug] " + this.getName() + ": " + message);
	}

	@Override
	public void debug(final Object message, final Throwable e) {
		this.shadowedLogger.debug(message, e);
		this.log("[debug] " + this.getName() + ": " + message, e);
	}

	@Override
	public void error(final Object message) {
		this.shadowedLogger.error(message);
		this.log("[error] " + this.getName() + ": " + message);
	}

	@Override
	public void error(final Object message, final Throwable e) {
		this.shadowedLogger.error(message, e);
		this.log("[error] " + this.getName() + ": " + message, e);
	}

	@Override
	public void fatal(final Object message) {
		this.shadowedLogger.fatal(message);
		this.log("[fatal] " + this.getName() + ": " + message);
	}

	@Override
	public void fatal(final Object message, final Throwable e) {
		this.shadowedLogger.fatal(message, e);
		this.log("[fatal] " + this.getName() + ": " + message, e);
	}

	@Override
	public void info(final Object message) {
		this.shadowedLogger.info(message);
		this.log("[info] " + this.getName() + ": " + message);
	}

	@Override
	public void info(final Object message, final Throwable e) {
		this.shadowedLogger.info(message, e);
		this.log("[info] " + this.getName() + ": " + message, e);
	}

	@Override
	public void warn(final Object message) {
		this.shadowedLogger.warn(message);
		this.log("[warn] " + this.getName() + ": " + message);
	}

	@Override
	public void warn(final Object message, final Throwable e) {
		this.shadowedLogger.warn(message, e);
		this.log("[warn] " + this.getName() + ": " + message, e);
	}

	/**
	 * Log a message object with the {@link org.apache.log4j.Level#TRACE TRACE}
	 * level.
	 *
	 * @param message
	 *            the message object to log.
	 * @see #debug(Object) for an explanation of the logic applied.
	 * @since 1.2.12
	 */
	@Override
	public void trace(final Object message) {
		this.shadowedLogger.trace(message);
		this.log("[TRACE] " + this.getName() + ": " + message);
	}

	/**
	 * Log a message object with the <code>TRACE</code> level including the
	 * stack trace of the {@link Throwable}<code>t</code> passed as parameter.
	 *
	 * <p>
	 * See {@link #debug(Object)} form for more detailed information.
	 * </p>
	 *
	 * @param message
	 *            the message object to log.
	 * @param t
	 *            the exception to log, including its stack trace.
	 * @since 1.2.12
	 */
	@Override
	public void trace(final Object message, final Throwable t) {
		this.shadowedLogger.trace(message, t);
		this.log(message, t);
	}

}
