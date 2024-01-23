package org.apache.calcite.adapter.gremlin.driver;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.*;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.jsr223.console.GremlinShellEnvironment;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.jsr223.console.RemoteException;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.apache.tinkerpop.gremlin.util.Tokens;

import javax.security.sasl.SaslException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

public class GremlinRemoteDriver implements RemoteAcceptor {

  public static final int NO_TIMEOUT = 0;
  public static final String USER_AGENT = "Gremlin Console/" + Gremlin.version();

  private Cluster currentCluster;
  private Client currentClient;
  private int timeout = NO_TIMEOUT;
  private Map<String,String> aliases = new HashMap<>();
  private Optional<String> session = Optional.empty();

  private static final String TOKEN_RESET = "reset";
  private static final String TOKEN_SHOW = "show";

  private static final String TOKEN_NONE = "none";
  private static final String TOKEN_TIMEOUT = "timeout";
  private static final String TOKEN_ALIAS = "alias";
  private static final String TOKEN_SESSION = "session";
  private static final String TOKEN_SESSION_MANAGED = "session-managed";
  private static final String TOKEN_HELP = "help";
  private static final List<String> POSSIBLE_TOKENS = Arrays.asList(TOKEN_TIMEOUT, TOKEN_ALIAS, TOKEN_HELP);

  private final GremlinShellEnvironment shellEnvironment;

  public GremlinRemoteDriver(final GremlinShellEnvironment shellEnvironment) {
    this.shellEnvironment = shellEnvironment;
  }

  @Override
  public Object connect(final List<String> args) throws RemoteException {
    if (args.size() < 1) throw new RemoteException("Expects the location of a configuration file or variable name for a Cluster object as an argument");

    try {
      final String fileOrVar = args.get(0);
      if (new File(fileOrVar).isFile())
        this.currentCluster = Cluster.open(fileOrVar);
      else if (shellEnvironment.getVariable(fileOrVar) != null) {
        final Object o = shellEnvironment.getVariable(fileOrVar);
        if (o instanceof Cluster) {
          this.currentCluster = (Cluster) o;
        }
      }

      if (null == currentCluster)
        throw new RemoteException("Expects the location of a configuration file or variable name for a Cluster object as an argument");
      final boolean useSession = args.size() >= 2 && (args.get(1).equals(TOKEN_SESSION) || args.get(1).equals(TOKEN_SESSION_MANAGED));
      if (useSession) {
        final String sessionName = args.size() == 3 ? args.get(2) : UUID.randomUUID().toString();
        session = Optional.of(sessionName);

        final boolean managed = args.get(1).equals(TOKEN_SESSION_MANAGED);

        this.currentClient = this.currentCluster.connect(sessionName, managed);
      } else {
        this.currentClient = this.currentCluster.connect();
      }
      this.currentClient.init();
      return String.format("Configured %s", this.currentCluster) + getSessionStringSegment();
    } catch (final FileNotFoundException ignored) {
      throw new RemoteException("The 'connect' option must be accompanied by a valid configuration file");
    } catch (final NoHostAvailableException nhae) {
      return String.format("Configured %s, but host not presently available. The Gremlin Console will attempt to reconnect on each query submission, " +
          "however, you may wish to close this remote and investigate the problem directly. %s", this.currentCluster, getSessionStringSegment());
    } catch (final Exception ex) {
      throw new RemoteException("Error during 'connect' - " + ex.getMessage(), ex);
    }
  }

  @Override
  public Object configure(final List<String> args) throws RemoteException {
    final String option = args.size() == 0 ? "" : args.get(0);
    if (!POSSIBLE_TOKENS.contains(option))
      throw new RemoteException(String.format("The 'config' option expects one of ['%s'] as an argument", String.join(",", POSSIBLE_TOKENS)));

    final List<String> arguments = args.subList(1, args.size());

    if (option.equals(TOKEN_HELP)) {
      return ":remote config [timeout [<ms>|none]|alias [reset|show|<alias> <actual>]|help]";
    } else if (option.equals(TOKEN_TIMEOUT)) {
      final String errorMessage = "The timeout option expects a positive integer representing milliseconds or 'none' as an argument";
      if (arguments.size() != 1) throw new RemoteException(errorMessage);
      try {
        // first check for MAX timeout then NONE and finally parse the config to int.
        timeout = arguments.get(0).equals(TOKEN_NONE) ? NO_TIMEOUT : Integer.parseInt(arguments.get(0));
        if (timeout < NO_TIMEOUT) throw new RemoteException("The value for the timeout cannot be less than " + NO_TIMEOUT);
        return timeout == NO_TIMEOUT ? "Remote timeout is disabled" : "Set remote timeout to " + timeout + "ms";
      } catch (Exception ignored) {
        throw new RemoteException(errorMessage);
      }
    } else if (option.equals(TOKEN_ALIAS)) {
      if (arguments.size() == 1 && arguments.get(0).equals(TOKEN_RESET)) {
        aliases.clear();
        return "Aliases cleared";
      }

      if (arguments.size() == 1 && arguments.get(0).equals(TOKEN_SHOW)) {
        return aliases;
      }

      if (arguments.size() % 2 != 0)
        throw new RemoteException("Arguments to alias must be 'reset' to clear any existing alias settings or key/value alias/binding pairs");

      final Map<String,Object> aliasMap = ElementHelper.asMap(arguments.toArray());
      aliases.clear();
      aliasMap.forEach((k,v) -> aliases.put(k, v.toString()));
      return aliases;
    }

    return this.toString();
  }

  @Override
  public Object submit(final List<String> args) throws RemoteException {
    final String line = getScript(String.join(" ", args), this.shellEnvironment);

    try {
      final List<Result> resultSet = send(line);
      this.shellEnvironment.setVariable(RESULT, resultSet);
      return resultSet.stream().map(result -> result.getObject()).iterator();
    } catch (SaslException sasl) {
      throw new RemoteException("Security error - check username/password and related settings", sasl);
    } catch (Exception ex) {
      // users may try to submit after initiating cluster on a dead host, if host remains unavailable after initiation, output this error message
      if (ex.getCause().getCause() instanceof NoHostAvailableException) {
        return String.format("Configured %s, but host not presently available. The Gremlin Console will attempt to reconnect on each query submission, " +
            "however, you may wish to close this remote and investigate the problem directly. %s", this.currentCluster, getSessionStringSegment());
      }
      final Optional<ResponseException> inner = findResponseException(ex);
      if (inner.isPresent()) {
        final ResponseException responseException = inner.get();
        if (responseException.getResponseStatusCode() == ResponseStatusCode.SERVER_ERROR_TIMEOUT) {
          throw new RemoteException(String.format("%s - try increasing the timeout with the :remote command", responseException.getMessage()));
        } else if (responseException.getResponseStatusCode() == ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
          throw new RemoteException(String.format(
              "Server could not serialize the result requested. Server error - %s. Note that the class must be serializable by the client and server for proper operation.", responseException.getMessage()),
              responseException.getRemoteStackTrace().orElse(null));
        else
          throw new RemoteException(responseException.getMessage(), responseException.getRemoteStackTrace().orElse(null));
      } else if (ex.getCause() != null) {
        final Throwable rootCause = ExceptionUtils.getRootCause(ex);
        if (rootCause instanceof TimeoutException)
          throw new RemoteException("Host did not respond in a timely fashion - check the server status and submit again.");
        else
          throw new RemoteException(rootCause.getMessage());
      } else {
        throw new RemoteException(ex.getMessage());
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (this.currentClient != null) this.currentClient.close();
    if (this.currentCluster != null) this.currentCluster.close();
  }

  public int getTimeout() {
    return timeout;
  }

  private List<Result> send(final String gremlin) throws SaslException {
    try {
      final RequestOptions.Builder options = RequestOptions.build();
      aliases.forEach(options::addAlias);
      if (timeout > NO_TIMEOUT)
        options.timeout(timeout);

      options.userAgent(USER_AGENT);

      final ResultSet rs = this.currentClient.submit(gremlin, options.create());
      final List<Result> results = rs.all().get();
      final Map<String, Object> statusAttributes = rs.statusAttributes().getNow(null);

      // Check for and print warnings
      if (null != statusAttributes && statusAttributes.containsKey(Tokens.STATUS_ATTRIBUTE_WARNINGS)) {
        final Object warningAttributeObject = statusAttributes.get(Tokens.STATUS_ATTRIBUTE_WARNINGS);
        if (warningAttributeObject instanceof List) {
          for (Object warningListItem : (List<?>)warningAttributeObject)
            shellEnvironment.errPrintln(String.valueOf(warningListItem));
        } else {
          shellEnvironment.errPrintln(String.valueOf(warningAttributeObject));
        }
      }

      return results;
    } catch (Exception e) {
      // handle security error as-is and unwrapped
      final Optional<Throwable> throwable  = Stream.of(ExceptionUtils.getThrowables(e)).filter(t -> t instanceof SaslException).findFirst();
      if (throwable.isPresent())
        throw (SaslException) throwable.get();

      throw new IllegalStateException(e.getMessage(), e);
    }
  }

  @Override
  public boolean allowRemoteConsole() {
    return true;
  }

  @Override
  public String toString() {
    return "Gremlin Server - [" + this.currentCluster + "]" + getSessionStringSegment();
  }

  private Optional<ResponseException> findResponseException(final Throwable ex) {
    if (ex instanceof ResponseException)
      return Optional.of((ResponseException) ex);

    if (null == ex.getCause())
      return Optional.empty();

    return findResponseException(ex.getCause());
  }

  private String getSessionStringSegment() {
    return session.isPresent() ? String.format("-[%s]", session.get()) : "";
  }

  /**
   * Retrieve a script as defined in the shell context.  This allows for multi-line scripts to be submitted.
   */
  public static String getScript(final String submittedScript, final GremlinShellEnvironment shellEnvironment) {
    return submittedScript.startsWith("@") ? shellEnvironment.getVariable(submittedScript.substring(1)).toString() : submittedScript;
  }

}
