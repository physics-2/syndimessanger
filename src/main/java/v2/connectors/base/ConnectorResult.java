package v2.connectors.base;

public record ConnectorResult(boolean success, String message) {
    public static ConnectorResult ok(String msg)  { return new ConnectorResult(true, msg); }
    public static ConnectorResult fail(String msg){ return new ConnectorResult(false, msg); }
}