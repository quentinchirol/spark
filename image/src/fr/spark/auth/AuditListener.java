package fr.spark.auth;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.connect.service.SparkListenerConnectOperationCanceled;
import org.apache.spark.sql.connect.service.SparkListenerConnectOperationClosed;
import org.apache.spark.sql.connect.service.SparkListenerConnectOperationFailed;
import org.apache.spark.sql.connect.service.SparkListenerConnectOperationFinished;
import org.apache.spark.sql.connect.service.SparkListenerConnectOperationStarted;
import org.apache.spark.sql.connect.service.SparkListenerConnectSessionStarted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Journal d'audit : une ligne JSON par session ouverte et par requête (début, fin, échec),
 * avec l'utilisateur authentifié par {@link TokenAuthInterceptor}.
 */
public final class AuditListener extends SparkListener {

  private static final Logger AUDIT = LoggerFactory.getLogger("spark.connect.audit");
  private static final int MAX_TEXT = 2000;

  private final Map<String, String> userByOperation = new ConcurrentHashMap<>();

  @Override
  public void onOtherEvent(SparkListenerEvent event) {
    if (event instanceof SparkListenerConnectSessionStarted e) {
      log(e.eventTime(), "SESSION_START", e.userId(), "session", e.sessionId());
    } else if (event instanceof SparkListenerConnectOperationStarted e) {
      userByOperation.put(e.operationId(), e.userId());
      log(e.eventTime(), "QUERY_START", e.userId(), "session", e.sessionId(),
          "operation", e.operationId(), "statement", e.statementText());
    } else if (event instanceof SparkListenerConnectOperationFinished e) {
      log(e.eventTime(), "QUERY_END", userOf(e.operationId()), "operation", e.operationId(),
          "rows", e.producedRowCount().isDefined() ? e.producedRowCount().get().toString() : "");
    } else if (event instanceof SparkListenerConnectOperationFailed e) {
      log(e.eventTime(), "QUERY_FAILED", userOf(e.operationId()), "operation", e.operationId(),
          "error", e.errorMessage());
    } else if (event instanceof SparkListenerConnectOperationCanceled e) {
      log(e.eventTime(), "QUERY_CANCELED", userOf(e.operationId()), "operation", e.operationId());
    } else if (event instanceof SparkListenerConnectOperationClosed e) {
      userByOperation.remove(e.operationId());
    }
  }

  private String userOf(String operationId) {
    return userByOperation.getOrDefault(operationId, "?");
  }

  private static void log(long time, String type, String user, String... fields) {
    StringBuilder json = new StringBuilder("{")
        .append("\"time\":\"").append(Instant.ofEpochMilli(time)).append('"')
        .append(",\"event\":\"").append(type).append('"')
        .append(",\"user\":\"").append(escape(user)).append('"');
    for (int i = 0; i + 1 < fields.length; i += 2) {
      json.append(",\"").append(fields[i]).append("\":\"").append(escape(fields[i + 1])).append('"');
    }
    AUDIT.info("AUDIT {}", json.append('}'));
  }

  private static String escape(String value) {
    if (value == null) {
      return "";
    }
    String compact = value.replaceAll("\\s+", " ").trim();
    if (compact.length() > MAX_TEXT) {
      compact = compact.substring(0, MAX_TEXT) + "...";
    }
    return compact.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
