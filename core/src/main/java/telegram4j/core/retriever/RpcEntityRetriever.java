package telegram4j.core.retriever;

import reactor.core.publisher.Mono;
import telegram4j.core.MTProtoTelegramClient;
import telegram4j.core.auxiliary.AuxiliaryMessages;
import telegram4j.core.object.Id;
import telegram4j.core.object.PeerEntity;
import telegram4j.core.object.PeerId;
import telegram4j.core.object.User;
import telegram4j.core.object.chat.Chat;
import telegram4j.core.spec.IdFields;
import telegram4j.core.util.AuxiliaryEntityFactory;
import telegram4j.core.util.EntityFactory;
import telegram4j.mtproto.service.ServiceHolder;
import telegram4j.mtproto.store.StoreLayout;
import telegram4j.mtproto.util.TlEntityUtil;
import telegram4j.tl.*;
import telegram4j.tl.messages.MessagesNotModified;

import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class RpcEntityRetriever implements EntityRetriever {

    private final MTProtoTelegramClient client;
    private final ServiceHolder serviceHolder;
    private final StoreLayout storeLayout;

    public RpcEntityRetriever(MTProtoTelegramClient client) {
        this.client = Objects.requireNonNull(client, "client");
        this.serviceHolder = client.getServiceHolder();
        this.storeLayout = client.getMtProtoResources().getStoreLayout();
    }

    @Override
    public Mono<PeerEntity> resolvePeer(PeerId peerId) {
        Mono<PeerEntity> resolveById = Mono.justOrEmpty(peerId.asId())
                .flatMap(id -> {
                    switch (id.getType()) {
                        case CHAT:
                        case CHANNEL: return getChatMinById(id);
                        case USER: return getUserMinById(id);
                        default: return Mono.error(new IllegalStateException());
                    }
                });

        return Mono.justOrEmpty(peerId.asUsername())
                .map(TlEntityUtil::stripUsername)
                .flatMap(username -> storeLayout.resolvePeer(username)
                        .switchIfEmpty(serviceHolder.getUserService()
                                .resolveUsername(username)))
                .map(p -> EntityFactory.createPeerEntity(client, p))
                .switchIfEmpty(resolveById);
    }

    @Override
    public Mono<User> getUserMinById(Id userId) {
        if (userId.getType() != Id.Type.USER) {
            return Mono.error(new IllegalArgumentException("Incorrect id type, expected: "
                    + Id.Type.USER + ", but given: " + userId.getType()));
        }

        return storeLayout.getUserMinById(userId.asLong())
                .switchIfEmpty(asInputUser(userId).flatMap(serviceHolder.getUserService()::getUser))
                .ofType(BaseUser.class)
                .map(u -> new User(client, u));
    }

    @Override
    public Mono<User> getUserFullById(Id userId) {
        if (userId.getType() != Id.Type.USER) {
            return Mono.error(new IllegalArgumentException("Incorrect id type, expected: "
                    + Id.Type.USER + ", but given: " + userId.getType()));
        }

        return storeLayout.getUserFullById(userId.asLong())
                .switchIfEmpty(asInputUser(userId).flatMap(serviceHolder.getUserService()::getFullUser))
                .map(u -> EntityFactory.createUser(client, u));
    }

    @Override
    public Mono<Chat> getChatMinById(Id chatId) {
        if (chatId.getType() == Id.Type.USER) {
            return Mono.error(new IllegalArgumentException("Incorrect id type, expected: [CHANNEL, CHAT], but given: USER"));
        }

        return storeLayout.getChatMinById(chatId.asLong())
                .switchIfEmpty(Mono.defer(() -> {
                    if (chatId.getType() == Id.Type.CHAT) {
                        return serviceHolder.getChatService().getChat(chatId.asLong());
                    }
                    return asInputChannel(chatId).flatMap(serviceHolder.getChatService()::getChannel);
                }))
                .ofType(telegram4j.tl.Chat.class)
                .map(c -> EntityFactory.createChat(client, c, null));
    }

    @Override // TODO: or also try fetch user?
    public Mono<Chat> getChatFullById(Id chatId) {
        if (chatId.getType() == Id.Type.USER) {
            return Mono.error(new IllegalArgumentException("Incorrect id type, expected: [CHANNEL, CHAT], but given: USER"));
        }

        return storeLayout.getChatFullById(chatId.asLong())
                .switchIfEmpty(Mono.defer(() -> {
                    if (chatId.getType() == Id.Type.CHAT) {
                        return serviceHolder.getChatService().getFullChat(chatId.asLong());
                    }
                    return asInputChannel(chatId).flatMap(serviceHolder.getChatService()::getFullChannel);
                }))
                .map(c -> EntityFactory.createChat(client, c, null));
    }

    @Override
    public Mono<AuxiliaryMessages> getMessagesById(Iterable<? extends IdFields.MessageId> messageIds) {
        return Mono.defer(() -> {
            var ids = StreamSupport.stream(messageIds.spliterator(), false)
                    .map(IdFields.MessageId::asInputMessage)
                    .collect(Collectors.toList());

            return storeLayout.getMessages(ids)
                    .switchIfEmpty(client.getServiceHolder()
                            .getMessageService()
                            .getMessages(ids))
                    .filter(m -> m.identifier() != MessagesNotModified.ID) // just ignore
                    .map(d -> AuxiliaryEntityFactory.createMessages(client, d));
        });
    }


    @Override
    public Mono<AuxiliaryMessages> getMessagesById(Id channelId, Iterable<? extends IdFields.MessageId> messageIds) {
        if (channelId.getType() != Id.Type.CHANNEL) {
            return Mono.error(new IllegalArgumentException("Incorrect id type, expected: "
                    + Id.Type.CHANNEL + ", but given: " + channelId.getType()));
        }

        return Mono.defer(() -> {
            var ids = StreamSupport.stream(messageIds.spliterator(), false)
                    .map(IdFields.MessageId::asInputMessage)
                    .collect(Collectors.toList());

            return storeLayout.getMessages(channelId.asLong(), ids)
                    .switchIfEmpty(asInputChannel(channelId)
                            .flatMap(c -> client.getServiceHolder()
                                    .getMessageService()
                                    .getMessages(c, ids)))
                    .filter(m -> m.identifier() != MessagesNotModified.ID) // just ignore
                    .map(d -> AuxiliaryEntityFactory.createMessages(client, d));
        });
    }

    private Mono<InputPeer> asInputPeer(Id chatId) {
        switch (chatId.getType()) {
            case USER: return asInputUser(chatId).map(TlEntityUtil::toInputPeer);
            case CHAT: return Mono.just(ImmutableInputPeerChat.of(chatId.asLong()));
            case CHANNEL: return asInputChannel(chatId).map(TlEntityUtil::toInputPeer);
            default: throw new IllegalStateException();
        }
    }

    private Mono<InputChannel> asInputChannel(Id channelId) {
        if (channelId.getAccessHash().isEmpty()) {
            return storeLayout.resolveChannel(channelId.asLong());
        }
        return Mono.just(ImmutableBaseInputChannel.of(channelId.asLong(), channelId.getAccessHash().orElseThrow()));
    }


    private Mono<InputUser> asInputUser(Id userId) {
        if (userId.getAccessHash().isEmpty()) {
            return storeLayout.resolveUser(userId.asLong());
        }
        return Mono.just(ImmutableBaseInputUser.of(userId.asLong(), userId.getAccessHash().orElseThrow()));
    }
}
