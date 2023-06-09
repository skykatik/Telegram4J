package telegram4j.mtproto.client.impl;

import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import telegram4j.mtproto.DataCenter;
import telegram4j.mtproto.auth.AuthKey;
import telegram4j.tl.api.TlMethod;
import telegram4j.tl.mtproto.MessageContainer;
import telegram4j.tl.mtproto.MsgResendReq;
import telegram4j.tl.mtproto.MsgsAck;
import telegram4j.tl.mtproto.MsgsStateReq;
import telegram4j.tl.request.mtproto.Ping;
import telegram4j.tl.request.mtproto.PingDelayDisconnect;

import java.util.Objects;
import java.util.StringJoiner;

import static telegram4j.mtproto.util.CryptoUtil.random;

public final class AuthData {
    private static final int DEFAULT_IMSG_ID_REGISTER_SIZE = 128;

    @Nullable
    private AuthKey authKey;
    private int timeOffset;
    private long lastMessageId;
    private long oldSessionId;
    private long sessionId = random.nextLong();
    private long serverSalt;
    private int seqNo;
    private boolean unauthorized;

    private final DataCenter dc;
    private final InboundMessageIdRegister messageIdRegister = new InboundMessageIdRegister(DEFAULT_IMSG_ID_REGISTER_SIZE);

    public AuthData(DataCenter dc) {
        this.dc = Objects.requireNonNull(dc);
    }

    public void updateTimeOffset(int serverTime) {
        int now = Math.toIntExact(System.currentTimeMillis() / 1000);
        int updated = serverTime - now;
        if (Math.abs(timeOffset - updated) > 3) {
            lastMessageId = 0;
            timeOffset = updated;
        }
    }

    public long nextMessageId() {
        long millis = System.currentTimeMillis();
        long seconds = millis / 1000;
        long mod = millis % 1000;
        // [ 32 bits to approximate server time in seconds | 12 bits to fractional part of time | 20 bits of random number (divisible by 4) ]
        long messageId = seconds + timeOffset << 32 | mod << 20 | random.nextInt(0x1fffff) << 2;
        long l = lastMessageId;
        if (l >= messageId) {
            messageId = l + 4;
        }

        lastMessageId = messageId;
        return messageId;
    }

    public int nextSeqNo(TlMethod<?> object) {
        return nextSeqNo(isContentRelated(object));
    }

    public int nextSeqNo(boolean content) {
        if (content) {
            return seqNo++ * 2 + 1;
        }
        return seqNo * 2;
    }

    public long serverSalt() {
        return serverSalt;
    }

    public void serverSalt(long serverSalt) {
        this.serverSalt = serverSalt;
    }

    public long sessionId() {
        return sessionId;
    }

    public void resetSessionId() {
        seqNo = 0;
        oldSessionId = sessionId;

        do {
            sessionId = random.nextLong();
        } while (sessionId == oldSessionId);
    }

    public boolean unauthorized() {
        return unauthorized;
    }

    public void unauthorized(boolean state) {
        unauthorized = state;
    }

    @Nullable
    public AuthKey authKey() {
        return authKey;
    }

    public void authKey(AuthKey authKey) {
        this.authKey = authKey;
    }

    public void lastMessageId(long firstMsgId) {
        this.lastMessageId = firstMsgId;
    }

    public DataCenter dc() {
        return dc;
    }

    public void timeOffset(int timeOffset) {
        this.timeOffset = timeOffset;
    }

    // https://github.com/tdlib/td/blob/4041ecb535802ba1c55fcd11adf5d3ada41c2be7/td/mtproto/AuthData.cpp#L132
    @Nullable
    public InvalidInboundMsgIdReason isValidInboundMessageId(long messageId) {
        if ((messageId & 1) == 0) {
            return InvalidInboundMsgIdReason.EVEN;
        }

        long serverTime = timeOffset + (System.currentTimeMillis()/1000);
        long timeFromMessageId = messageId / (1L << 32);
        boolean timeCheck = serverTime - 300 < timeFromMessageId && timeFromMessageId < serverTime + 30;
        if (!timeCheck) {
            return InvalidInboundMsgIdReason.INVALID_TIME;
        }
        if (!messageIdRegister.check(messageId)) {
            return InvalidInboundMsgIdReason.DUPLICATE;
        }
        return null;
    }

    public long oldSessionId() {
        return oldSessionId;
    }

    public enum InvalidInboundMsgIdReason {
        EVEN,
        INVALID_TIME,
        DUPLICATE
    }

    static boolean isContentRelated(TlMethod<?> object) {
        return switch (object.identifier()) {
            case MsgsAck.ID:
            case Ping.ID:
            case PingDelayDisconnect.ID:
            case MessageContainer.ID:
            case MsgsStateReq.ID:
            case MsgResendReq.ID:
                yield false;
            default:
                yield true;
        };
    }

    static class InboundMessageIdRegister {

        private static final Logger log = Loggers.getLogger("telegram4j.mtproto.InboundMessageIdRegister");

        // The implementation of ordered set
        // with fixed size. If new messageId's are added
        // then the oldest id of set will be replaced by new value.

        private final long[] buffer;
        private int pos;
        private boolean overflow;

        public InboundMessageIdRegister(int size) {
            // The buffer will not work correctly on very small sizes,
            // so I use sizes greater than 16

            assert size > 1 && (size & size - 1) == 0;
            buffer = new long[size];
        }

        public boolean check(long messageId) {
            assert messageId != 0;

            int p = pos;
            int min = overflow ? p : 0;
            long oldest = buffer[min == buffer.length ? 0 : min];
            if (messageId <= oldest) { // messageId is less than minId - ignore (return false)
                return false;
            }

            int max = p - 1;
            if (max == -1) { // there is no maxId (empty buffer) - enqueue (return true)
                buffer[pos++] = messageId;
                return true;
            }

            long d = messageId - buffer[max];
            if (d > 0) { // messageId is greater than maxId - enqueue (return true)
                if (p == buffer.length) {
                    overflow = true;
                    pos = 0;
                }

                buffer[pos++] = messageId;
                return true;
            } else if (d == 0) { // messageId is equals to maxId - already received (return false)
                return false;
            } else { // d < 0; messageId is less than maxId
                // There is two kind of situation:
                // 1. messageId already received (return false)
                // 2. Received non-incremental inconsistent identifier. For now I consider it as protocol error

                for (long l : buffer) {
                    if (l == messageId) {
                        return false;
                    }
                }
                if (log.isDebugEnabled()) {
                    StringJoiner dump = new StringJoiner(", ", "{", "}");
                    for (long msgId : buffer) {
                        dump.add("0x" + Long.toHexString(msgId));
                    }

                    log.debug("Received non-incremental messageId. Current set: " + dump);
                }
                return true;
            }
        }
    }
}
