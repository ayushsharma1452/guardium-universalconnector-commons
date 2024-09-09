package com.ibm.guardium.universalconnector.commons.customparsing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ibm.guardium.universalconnector.commons.customparsing.regex.RegexExecutor;
import com.ibm.guardium.universalconnector.commons.customparsing.regex.RegexResult;
import com.ibm.guardium.universalconnector.commons.structures.*;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.ibm.guardium.universalconnector.commons.customparsing.PropertyConstant.*;

abstract class CustomParserNew {
    private static final Logger logger = LogManager.getLogger(CustomParserNew.class);
    private static final RegexExecutor executor = new RegexExecutor();
    private static final InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
    protected Map<String, String> properties;
    private final ObjectMapper mapper = new ObjectMapper();

    public CustomParserNew() {
    }

    public Record parseRecord(String payload) {
        properties = getProperties();
        if (properties == null || payload == null) return null;

        return extractRecord(payload);
    }

    private Record extractRecord(String payload) {
        Record record = new Record();

        setSessionId(record, payload);
        setClientPort(record, payload);
        setServerPort(record, payload);
        setDbUser(record, payload);
        setServerType(record, payload);
        setDbProtocol(record, payload);
        setExceptionTypeId(record, payload);
        setAppUserName(record, payload);
        setClientHostName(record, payload);
        setClientIp(record, payload);
        setClientIpv6(record, payload);
        setClientMac(record, payload);
        setClientOs(record, payload);
        setCommProtocol(record, payload);
        setConstruct(record, payload);
        setDbName(record, payload);
        setDbProtocolVersion(record, payload);
        setDescription(record, payload);
        setIsIpv6(record, payload);
        setMinDst(record, payload);
        setMinOffsetFromGMT(record, payload);
        setOriginalSqlCommand(record, payload);
        setOsUser(record, payload);
        setServerDescription(record, payload);
        setServerHostName(record, payload);
        setServerIp(record, payload);
        setServerIpv6(record, payload);
        setServerOs(record, payload);
        setServiceName(record, payload);
        setSourceProgram(record, payload);
        setSqlString(record, payload);
        setTimestamp(record, payload);

        // Set additional fields in the accessor object if necessary
        setAccessor(record, payload);

        return record;
    }

    protected void setSessionId(Record record, String payload) {
        String sessionId = getValue(payload, SESSION_ID);
        record.setSessionId(sessionId != null ? sessionId : DEFAULT_STRING);
    }

    protected void setClientPort(Record record, String payload) {
        if (record.getSessionId() == null || record.getSessionId().isEmpty()) {
            record.getSessionLocator().setClientPort(SessionLocator.PORT_DEFAULT);
        } else {
            String value = getValue(payload, CLIENT_PORT);
            record.getSessionLocator().setClientPort(value != null ? Integer.parseInt(value) : SessionLocator.PORT_DEFAULT);
        }
    }

    protected void setServerPort(Record record, String payload) {
        if (record.getSessionId() == null || record.getSessionId().isEmpty()) {
            record.getSessionLocator().setServerPort(SessionLocator.PORT_DEFAULT);
        } else {
            String value = getValue(payload, SERVER_PORT);
            record.getSessionLocator().setServerPort(value != null ? Integer.parseInt(value) : SessionLocator.PORT_DEFAULT);
        }
    }

    protected void setDbUser(Record record, String payload) {
        String value = getValue(payload, DB_USER);
        record.getAccessor().setDbUser(value != null ? value : DATABASE_NOT_AVAILABLE);
    }

    protected void setServerType(Record record, String payload) {
        String value = getValue(payload, SERVER_TYPE);
        record.getAccessor().setServerType(value != null ? value : DEFAULT_STRING);
    }

    protected void setDbProtocol(Record record, String payload) {
        String value = getValue(payload, DB_PROTOCOL);
        record.getAccessor().setDbProtocol(value != null ? value : DEFAULT_STRING);
    }

    // method to handle exception type and description
    protected void setExceptionTypeId(Record record, String payload) {
        String errorMessage = getValue(payload, DESCRIPTION);  // Get the error message
        String exceptionTypeId;
        // Determine if it's a login failure or a SQL error
        if (errorMessage != null && errorMessage.toLowerCase().contains("login failed")) {
            exceptionTypeId = "LOGIN_FAILED";
        } else {
            exceptionTypeId = "SQL_ERROR";
        }
        record.getException().setExceptionTypeId(exceptionTypeId);
        record.getException().setDescription(errorMessage != null ? errorMessage : DEFAULT_STRING);  // Set the exact error message
    }

    protected void setAppUserName(Record record, String payload) {
        String value = getValue(payload, APP_USER_NAME);
        record.setAppUserName(value != null ? value : DEFAULT_STRING);
    }

    protected void setClientHostName(Record record, String payload) {
        String value = getValue(payload, CLIENT_HOSTNAME);
        record.getAccessor().setClientHostName(value != null ? value : DEFAULT_STRING);
    }

    protected void setClientIp(Record record, String payload) {
        String value = getValue(payload, CLIENT_IP);
        record.getSessionLocator().setClientIp(value != null ? value : DEFAULT_IP);
    }

    protected void setClientIpv6(Record record, String payload) {
        String value = getValue(payload, CLIENT_IPV6);
        record.getSessionLocator().setClientIpv6(value != null ? value : DEFAULT_IPV6);
    }

    protected void setClientMac(Record record, String payload) {
        String value = getValue(payload, CLIENT_MAC);
        record.getAccessor().setClient_mac(value != null ? value : DEFAULT_STRING);
    }

    protected void setClientOs(Record record, String payload) {
        String value = getValue(payload, CLIENT_OS);
        record.getAccessor().setClientOs(value != null ? value : DEFAULT_STRING);
    }

    protected void setCommProtocol(Record record, String payload) {
        String value = getValue(payload, COMM_PROTOCOL);
        record.getAccessor().setCommProtocol(value != null ? value : DEFAULT_STRING);
    }

    protected void setConstruct(Record record, String payload) {
        String value = getValue(payload, CONSTRUCT);
        if (value != null) {
            try {
                Construct construct = mapper.readValue(value, Construct.class);
                record.getData().setConstruct(construct);
            } catch (IOException e) {
                logger.error("Error parsing construct JSON", e);
            }
        } else {
            record.getData().setConstruct(new Construct());
        }
    }

    protected void setDbName(Record record, String payload) {
        String value = getValue(payload, DB_NAME);
        record.setDbName(value != null ? value : DEFAULT_STRING);
    }

    protected void setDbProtocolVersion(Record record, String payload) {
        String value = getValue(payload, DB_PROTOCOL_VERSION);
        record.getAccessor().setDbProtocolVersion(value != null ? value : DEFAULT_STRING);
    }

    protected void setDescription(Record record, String payload) {
        String value = getValue(payload, DESCRIPTION);
        record.getException().setDescription(value != null ? value : DEFAULT_STRING);
    }

    protected void setIsIpv6(Record record, String payload) {
        String value = getValue(payload, IS_IPV6);
        record.getSessionLocator().setIpv6(value != null && Boolean.parseBoolean(value));
    }

    protected void setMinDst(Record record, String payload) {
        String value = getValue(payload, MIN_DST);
        record.getTime().setMinDst(value != null ? Integer.parseInt(value) : 0);
    }

    protected void setMinOffsetFromGMT(Record record, String payload) {
        String value = getValue(payload, MIN_OFFSET_FROM_GMT);
        record.getTime().setMinOffsetFromGMT(value != null ? Integer.parseInt(value) : 0);
    }

    protected void setOriginalSqlCommand(Record record, String payload) {
        String value = getValue(payload, ORIGINAL_SQL_COMMAND);
        record.getData().setOriginalSqlCommand(value != null ? value : DEFAULT_STRING);
    }

    protected void setOsUser(Record record, String payload) {
        String value = getValue(payload, OS_USER);
        record.getAccessor().setOsUser(value != null ? value : DEFAULT_STRING);
    }

    protected void setServerDescription(Record record, String payload) {
        String value = getValue(payload, SERVER_DESCRIPTION);
        record.getAccessor().setServerDescription(value != null ? value : DEFAULT_STRING);
    }

    protected void setServerHostName(Record record, String payload) {
        String value = getValue(payload, SERVER_HOSTNAME);
        record.getAccessor().setServerHostName(value != null ? value : DEFAULT_STRING);
    }

    protected void setServerIp(Record record, String payload) {
        String value = getValue(payload, SERVER_IP);
        record.getSessionLocator().setServerIp(value != null ? value : DEFAULT_IP);
    }

    protected void setServerIpv6(Record record, String payload) {
        String value = getValue(payload, SERVER_IPV6);
        record.getSessionLocator().setServerIpv6(value != null ? value : DEFAULT_IPV6);
    }

    protected void setServerOs(Record record, String payload) {
        String value = getValue(payload, SERVER_OS);
        record.getAccessor().setServerOs(value != null ? value : DEFAULT_STRING);
    }

    protected void setServiceName(Record record, String payload) {
        String value = getValue(payload, SERVICE_NAME);
        record.getAccessor().setServiceName(value != null ? value : DEFAULT_STRING);
    }

    protected void setSourceProgram(Record record, String payload) {
        String value = getValue(payload, SOURCE_PROGRAM);
        record.getAccessor().setSourceProgram(value != null ? value : DEFAULT_STRING);
    }

    // method to handle the SQL command that caused the exception
    protected void setSqlString(Record record, String payload) {
        String value = getValue(payload, SQL_STRING);
        record.getException().setSqlString(value != null ? value : DEFAULT_STRING);  // Set the SQL command that caused the exception
    }

    // In this setTimestamp method now parses the timestamp from the payload and sets the timestamp, minOffsetFromGMT, and minDst fields in the Time object of the Record. If the timestamp is not available, it sets default values.
    protected void setTimestamp(Record record, String payload) {
        String value = getValue(payload, TIMESTAMP);
        if (value != null) {
            Time time = parseTimestamp(value);
            record.setTime(time);
        } else {
            record.getTime().setTimstamp(0L);
            record.getTime().setMinOffsetFromGMT(0);
            record.getTime().setMinDst(0);
        }
    }

    protected String getValue(String payload, String fieldName) {
        return parse(payload, properties.get(fieldName));
    }

    protected String parse(String payload, String regexString) {
        if (regexString == null) {
            return null;
        }
        Pattern pattern = Pattern.compile(regexString);
        RegexResult rr = executor.find(pattern, payload);
        if (rr.matched()) {
            Matcher m = rr.getMatcher();
            return m.groupCount() > 0 ? m.group(1) : m.group();
        } else {
            if (rr.timedOut() && logger.isDebugEnabled()) {
                logger.debug("Regex parse aborted due to taking too long to match -- regex: {}, event-payload: {}", pattern, payload);
            }
            return null;
        }
    }

    // Updated method to check accessor.dataType and populate original_sql_command or construct
    protected void setAccessor(Record record, String payload) {
        Accessor accessor = new Accessor();

        String accessorDataType = getValue(payload, ACCESSOR_TYPE); // Get accessor dataType field
        accessor.setDataType(accessorDataType != null ? accessorDataType : UNKOWN_STRING);

        if (Accessor.DATA_TYPE_GUARDIUM_SHOULD_PARSE_SQL.equalsIgnoreCase(accessor.getDataType())) {
            // Populate original_sql_command if dataType is "TEXT"
            String sqlCommand = getValue(payload, ORIGINAL_SQL_COMMAND);
            record.getData().setOriginalSqlCommand(sqlCommand != null ? sqlCommand : DEFAULT_STRING);
        }

        if (Accessor.DATA_TYPE_GUARDIUM_SHOULD_NOT_PARSE_SQL.equalsIgnoreCase(accessor.getDataType())) {
            // Populate construct if dataType is "CONSTRUCT"
            accessor.setLanguage(Accessor.LANGUAGE_FREE_TEXT_STRING);
            setConstruct(record, payload);
        }

        //null checks for accessor
        accessor.setDbUser(getValue(payload, DB_USER) != null ? getValue(payload, DB_USER) : DATABASE_NOT_AVAILABLE);
        accessor.setServerType(getValue(payload, SERVER_TYPE) != null ? getValue(payload, SERVER_TYPE) : DEFAULT_STRING);

        accessor.setServerOs(UNKOWN_STRING);
        accessor.setClientHostName(UNKOWN_STRING);
        accessor.setCommProtocol(UNKOWN_STRING);
        accessor.setDbProtocolVersion(UNKOWN_STRING);
        accessor.setOsUser(UNKOWN_STRING);
        accessor.setSourceProgram(UNKOWN_STRING);
        accessor.setClient_mac(UNKOWN_STRING);
        accessor.setServerDescription(UNKOWN_STRING);


        record.setAccessor(accessor);
    }


    private SessionLocator parseSessionLocator(String callerIp) {
        SessionLocator sessionLocator = new SessionLocator();
        sessionLocator.setIpv6(false); // Default to IPv4
        if (inetAddressValidator.isValidInet6Address(callerIp)) {
            // If client IP is IPv6, set both client and server to IPv6
            sessionLocator.setIpv6(true);
            sessionLocator.setClientIpv6(callerIp);
            sessionLocator.setServerIpv6(DEFAULT_IPV6); // Set server IP to default IPv6
        } else if (inetAddressValidator.isValidInet4Address(callerIp)) {
            // If client IP is IPv4, set both client and server IP to IPv4
            sessionLocator.setClientIp(callerIp);
            // Cloud Databases: Set server IP to 0.0.0.0
            sessionLocator.setServerIp("0.0.0.0");
        } else {
            // Default case when IP is invalid or not recognized
            sessionLocator.setClientIp(DEFAULT_IP);
            sessionLocator.setServerIp(DEFAULT_IP);
        }
        // Set port numbers
        sessionLocator.setClientPort(SessionLocator.PORT_DEFAULT == 0 ? -1 : SessionLocator.PORT_DEFAULT); // Set to -1 if port is missing
        sessionLocator.setServerPort(SessionLocator.PORT_DEFAULT == 0 ? -1 : SessionLocator.PORT_DEFAULT); // Set to -1 if port is missing
        // Additional check to ensure both IPs are IPv6 if necessary
        if (sessionLocator.isIpv6()) {
            if (sessionLocator.getClientIpv6() == null)
                sessionLocator.setClientIpv6(DEFAULT_IPV6);
            if (sessionLocator.getServerIpv6() == null)
                sessionLocator.setServerIpv6(DEFAULT_IPV6);
        }
        return sessionLocator;
    }

    public static Time parseTimestamp(String dateString) {
        ZonedDateTime date = ZonedDateTime.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
        long millis = date.toInstant().toEpochMilli();
        int minOffset = date.getOffset().getTotalSeconds() / 60;
        int minDst = date.getZone().getRules().isDaylightSavings(date.toInstant()) ? 60 : 0;
        return new Time(millis, minOffset, minDst);
    }

    public abstract String getConfigFilePath();

    public Map<String, String> getProperties() {
        try {
            String content = new String(Files.readAllBytes(Paths.get(getConfigFilePath())));
            return mapper.readValue(content, HashMap.class);
        } catch (IOException e) {
            logger.error("Error reading properties from config file", e);
            return null;
        }
    }
}

//  Added the database logic and the sniffer logic not a full logic just a starting point after the discussion in the
//  meeting need to update some lines and also need to add a few logic methods to exxtract the db info and the sql command or tables and instances etc.

/*

protected void setDbUser(Record record, String payload) {
        String value = getValue(payload, DB_USER);
        record.getAccessor().setDbUser(value != null ? value : "N.A.");
    }
    // Differentiates between server_type and db_protocol
    protected void setServerType(Record record, String payload) {
        String value = getValue(payload, SERVER_TYPE);
        record.getAccessor().setServerType(value != null ? value : DEFAULT_STRING);  // server_type: Database name (e.g., Oracle, MongoDB)
    }
    protected void setDbProtocol(Record record, String payload) {
        String value = getValue(payload, DB_PROTOCOL);
        record.getAccessor().setDbProtocol(value != null ? value : DEFAULT_STRING);  // db_protocol: Method used to gather audits (e.g., Logstash, Cloudwatch)
    }
    // method to handle Filbeat/syslog input plugins
    protected void setServerHostName(Record record, String payload) {
        String plugin = getValue(payload, INPUT_PLUGIN);
        if ("Filbert".equalsIgnoreCase(plugin) || "syslog".equalsIgnoreCase(plugin)) {
            // Fetch server details from Filebeat’s parameters
            String value = getValue(payload, SERVER_HOSTNAME);
            record.getAccessor().setServerHostName(value != null ? value : DEFAULT_STRING);
        } else {
            record.getAccessor().setServerHostName(DEFAULT_STRING);
        }
    }
    protected void setServerIp(Record record, String payload) {
        String plugin = getValue(payload, INPUT_PLUGIN);
        if ("Filbert".equalsIgnoreCase(plugin) || "syslog".equalsIgnoreCase(plugin)) {
            String value = getValue(payload, SERVER_IP);
            record.getSessionLocator().setServerIp(value != null ? value : DEFAULT_IP);
        } else {
            record.getSessionLocator().setServerIp(DEFAULT_IP);
        }
    }
    protected void setServerOs(Record record, String payload) {
        String plugin = getValue(payload, INPUT_PLUGIN);
        if ("Filbert".equalsIgnoreCase(plugin) || "syslog".equalsIgnoreCase(plugin)) {
            String value = getValue(payload, SERVER_OS);
            record.getAccessor().setServerOs(value != null ? value : UNKOWN_STRING);
        } else {
            record.getAccessor().setServerOs(UNKOWN_STRING);
        }
    }
    // Sniffer parser logic
    protected void setSnifferParser(Record record, boolean useSniffer) {
        if (useSniffer) {
            // Use sniffer parser
            record.getAccessor().setLanguage(record.getAccessor().getServerType());  // Set language to database mark
            record.getAccessor().setDataType(Accessor.DATA_TYPE_GUARDIUM_SHOULD_PARSE_SQL);  // Set type to "TEXT"
        } else {
            // Don't use sniffer parser
            record.getAccessor().setLanguage(Accessor.LANGUAGE_FREE_TEXT_STRING);  // Set language to "FREE_TEXT"
            record.getAccessor().setDataType(Accessor.DATA_TYPE_GUARDIUM_SHOULD_NOT_PARSE_SQL);  // Set type to "CONSTRUCT"
            setServerType(record, "CONSTRUCT");  // Set server_type to the database type
        }
    }

*/