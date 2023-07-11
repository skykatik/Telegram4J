package telegram4j.core.object.chat;

import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import telegram4j.core.MTProtoTelegramClient;
import telegram4j.core.object.BotInfo;
import telegram4j.core.retriever.EntityRetrievalStrategy;

import java.util.List;

/**
 * Represents an unlimited channel with messages broadcasting.
 *
 * @see <a href="https://core.telegram.org/api/channel#channels">Broadcasting Channels</a>
 */
public final class BroadcastChannel extends BaseChannel implements Channel, ChannelPeer {

    public BroadcastChannel(MTProtoTelegramClient client, telegram4j.tl.Channel minData) {
        super(client, minData);
    }

    public BroadcastChannel(MTProtoTelegramClient client, telegram4j.tl.ChannelFull fullData,
                            telegram4j.tl.Channel minData, @Nullable List<BotInfo> botInfo) {
        super(client, fullData, minData, botInfo);
    }

    @Override
    public Type getType() {
        return Type.CHANNEL;
    }

    @Override
    public Mono<SupergroupChat> getLinkedChannel() {
        return super.getLinkedChannel()
                .cast(SupergroupChat.class);
    }

    @Override
    public Mono<SupergroupChat> getLinkedChannel(EntityRetrievalStrategy strategy) {
        return super.getLinkedChannel(strategy)
                .cast(SupergroupChat.class);
    }

    @Override
    public String toString() {
        return "BroadcastChannel{" +
                "minData=" + minData +
                ", fullData=" + fullData +
                '}';
    }
}
