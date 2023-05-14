package org.example.Raft;

import org.apache.ratis.protocol.Message;

/**
 * The supported commands the Counter example.
 */
public enum SQLCommand {
    /** SQL command from client */
    SQL_COMMAND;

    private final Message message = Message.valueOf(name());

    public Message getMessage() {
        return message;
    }

    /** Does the given command string match this command? */
    public boolean matches(String command) {
        return name().equalsIgnoreCase(command);
    }
}
