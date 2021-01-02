package dev.efaust.collab.messaging;

import lombok.Data;

@Data
public class MessageHistoryEntry {
    String srcNode;
    String dstNode;
    Message message;
}
