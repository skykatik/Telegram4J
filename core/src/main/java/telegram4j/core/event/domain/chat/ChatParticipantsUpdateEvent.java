package telegram4j.core.event.domain.chat;

import reactor.util.annotation.Nullable;
import telegram4j.core.MTProtoTelegramClient;
import telegram4j.core.object.chat.ChatParticipant;
import telegram4j.core.object.chat.GroupChat;

import java.util.List;
import java.util.Optional;

public class ChatParticipantsUpdateEvent extends ChatEvent {

    private final GroupChat chat;
    @Nullable
    private final ChatParticipant selfParticipant;
    @Nullable
    private final Integer version;
    @Nullable
    private final List<ChatParticipant> participants;

    public ChatParticipantsUpdateEvent(MTProtoTelegramClient client, GroupChat chat,
                                       @Nullable ChatParticipant selfParticipant, @Nullable Integer version,
                                       @Nullable List<ChatParticipant> participants) {
        super(client);
        this.chat = chat;
        this.selfParticipant = selfParticipant;
        this.version = version;
        this.participants = participants;
    }

    public boolean isForbidden() {
        return version == null;
    }

    public GroupChat getChat() {
        return chat;
    }

    public Optional<ChatParticipant> getSelfParticipant() {
        return Optional.ofNullable(selfParticipant);
    }

    public Optional<Integer> getVersion() {
        return Optional.ofNullable(version);
    }

    public Optional<List<ChatParticipant>> getParticipants() {
        return Optional.ofNullable(participants);
    }

    @Override
    public String toString() {
        return "ChatParticipantsUpdateEvent{" +
                "chat=" + chat +
                ", selfParticipant=" + selfParticipant +
                ", version=" + version +
                ", participants=" + participants +
                '}';
    }
}
