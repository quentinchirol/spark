package fr.spark.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.connect.proto.UserContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sparkproject.connect.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import org.sparkproject.connect.grpc.Metadata;
import org.sparkproject.connect.grpc.ServerCall;
import org.sparkproject.connect.grpc.ServerCallHandler;
import org.sparkproject.connect.grpc.ServerInterceptor;
import org.sparkproject.connect.grpc.Status;
import org.sparkproject.connect.protobuf.Descriptors.FieldDescriptor;
import org.sparkproject.connect.protobuf.Message;

/**
 * Authentifie chaque appel Spark Connect avec un token personnel ("Authorization: Bearer ...")
 * et impose l'utilisateur correspondant dans la requête : le user_id envoyé par le client est
 * ignoré, on ne peut donc ni usurper une identité ni accéder à la session de quelqu'un d'autre.
 *
 * Les tokens sont lus dans un fichier "utilisateur=token" (une ligne par personne), rechargé
 * automatiquement quand il change.
 */
public final class TokenAuthInterceptor implements ServerInterceptor {

  private static final Logger LOG = LoggerFactory.getLogger(TokenAuthInterceptor.class);
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
  private static final String BEARER = "Bearer ";

  private final Path tokensFile;
  private FileTime loadedVersion;
  private volatile Map<String, String> usersByToken = Map.of();

  public TokenAuthInterceptor() {
    String path = System.getenv("SPARK_CONNECT_TOKENS_FILE");
    tokensFile = Path.of(path != null ? path : "/etc/spark-connect/tokens");
    reloadIfChanged();
    if (usersByToken.isEmpty()) {
      throw new IllegalStateException("Aucun token trouvé dans " + tokensFile);
    }
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    String user = authenticate(headers.get(AUTHORIZATION));
    if (user == null) {
      LOG.warn("Appel refusé (token absent ou invalide) : {}",
          call.getMethodDescriptor().getFullMethodName());
      call.close(Status.UNAUTHENTICATED.withDescription("Token absent ou invalide"), new Metadata());
      return new ServerCall.Listener<ReqT>() {};
    }
    return new SimpleForwardingServerCallListener<ReqT>(next.startCall(call, headers)) {
      @Override
      public void onMessage(ReqT message) {
        super.onMessage(withUser(message, user));
      }
    };
  }

  private String authenticate(String header) {
    if (header == null || !header.startsWith(BEARER)) {
      return null;
    }
    reloadIfChanged();
    byte[] given = header.substring(BEARER.length()).trim().getBytes(StandardCharsets.UTF_8);
    String user = null;
    // comparaison à temps constant sur tous les tokens, pour ne rien laisser deviner
    for (Map.Entry<String, String> e : usersByToken.entrySet()) {
      if (MessageDigest.isEqual(given, e.getKey().getBytes(StandardCharsets.UTF_8))) {
        user = e.getValue();
      }
    }
    return user;
  }

  /** Remplace le champ user_context de la requête par l'utilisateur authentifié. */
  @SuppressWarnings("unchecked")
  private static <T> T withUser(T message, String user) {
    if (!(message instanceof Message m)) {
      return message;
    }
    FieldDescriptor field = m.getDescriptorForType().findFieldByName("user_context");
    if (field == null) {
      return message;
    }
    UserContext context = ((UserContext) m.getField(field)).toBuilder()
        .setUserId(user)
        .setUserName(user)
        .build();
    return (T) m.toBuilder().setField(field, context).build();
  }

  private synchronized void reloadIfChanged() {
    try {
      FileTime version = Files.getLastModifiedTime(tokensFile);
      if (version.equals(loadedVersion)) {
        return;
      }
      Map<String, String> tokens = new HashMap<>();
      for (String line : Files.readAllLines(tokensFile, StandardCharsets.UTF_8)) {
        line = line.trim();
        int sep = line.indexOf('=');
        if (line.isEmpty() || line.startsWith("#") || sep <= 0) {
          continue;
        }
        tokens.put(line.substring(sep + 1).trim(), line.substring(0, sep).trim());
      }
      usersByToken = Map.copyOf(tokens);
      loadedVersion = version;
      LOG.info("{} utilisateur(s) chargé(s) depuis {} : {}",
          tokens.size(), tokensFile, tokens.values().stream().sorted().toList());
    } catch (IOException e) {
      LOG.error("Impossible de lire {}", tokensFile, e);
    }
  }
}
