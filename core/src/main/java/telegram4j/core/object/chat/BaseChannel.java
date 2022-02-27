package telegram4j.core.object.chat;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import telegram4j.core.MTProtoTelegramClient;
import telegram4j.core.object.BotInfo;
import telegram4j.core.object.ChatAdminRights;
import telegram4j.core.object.ChatPhoto;
import telegram4j.core.object.ExportedChatInvite;
import telegram4j.core.object.PeerNotifySettings;
import telegram4j.core.object.Photo;
import telegram4j.core.object.RestrictionReason;
import telegram4j.core.object.StickerSet;
import telegram4j.core.object.*;
import telegram4j.core.util.EntityFactory;
import telegram4j.core.util.PaginationSupport;
import telegram4j.mtproto.util.TlEntityUtil;
import telegram4j.tl.*;
import telegram4j.tl.channels.BaseChannelParticipants;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

abstract class BaseChannel extends BaseChat implements Channel {

    protected final telegram4j.tl.Channel minData;
    @Nullable
    protected final telegram4j.tl.ChannelFull fullData;
    @Nullable
    protected final ExportedChatInvite exportedChatInvite;

    protected BaseChannel(MTProtoTelegramClient client, Id id, Type type, telegram4j.tl.Channel minData) {
        super(client, id, type);
        this.minData = Objects.requireNonNull(minData, "minData");
        this.fullData = null;
        this.exportedChatInvite = null;
    }

    protected BaseChannel(MTProtoTelegramClient client, Id id, Type type,
                          ChannelFull fullData, telegram4j.tl.Channel minData,
                          @Nullable ExportedChatInvite exportedChatInvite) {
        super(client, id, type);
        this.minData = Objects.requireNonNull(minData, "minData");
        this.fullData = Objects.requireNonNull(fullData, "fullData");
        this.exportedChatInvite = exportedChatInvite;
    }

    @Override
    public EnumSet<Flag> getFlags() {
        return Flag.of(fullData, minData);
    }

    @Override
    public String getName() {
        return minData.title();
    }

    @Override
    public Optional<String> getUsername() {
        return Optional.ofNullable(minData.username());
    }

    @Override
    public Optional<String> getAbout() {
        return Optional.ofNullable(fullData).map(ChannelFull::about);
    }

    @Override
    public Optional<Integer> getPinnedMessageId() {
        return Optional.ofNullable(fullData).map(ChannelFull::pinnedMsgId);
    }

    @Override
    public Optional<ChatPhoto> getMinPhoto() {
        return Optional.ofNullable(TlEntityUtil.unmapEmpty(minData.photo(), BaseChatPhoto.class))
                .map(d -> new ChatPhoto(client, d, getIdAsPeer(), -1));
    }

    @Override
    public Optional<Photo> getPhoto() {
        return Optional.ofNullable(fullData)
                .map(d -> TlEntityUtil.unmapEmpty(d.chatPhoto(), BasePhoto.class))
                .map(d -> new Photo(client, d, ImmutableInputPeerChannel.of(minData.id(),
                        Objects.requireNonNull(minData.accessHash())), -1));
    }

    @Override
    public Optional<Duration> getMessageAutoDeleteDuration() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::ttlPeriod)
                .map(Duration::ofSeconds);
    }

    @Override
    public Instant getCreateTimestamp() {
        return Instant.ofEpochSecond(minData.date());
    }

    @Override
    public Optional<List<RestrictionReason>> getRestrictionReason() {
        return Optional.ofNullable(minData.restrictionReason())
                .map(list -> list.stream()
                        .map(d -> new RestrictionReason(client, d))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<List<BotInfo>> getBotInfo() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::botInfo)
                .map(list -> list.stream()
                        .map(d -> new BotInfo(client, d))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<StickerSet> getStickerSet() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::stickerset)
                .map(d -> new StickerSet(client, d));
    }

    @Override
    public Optional<PeerNotifySettings> getNotifySettings() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::notifySettings)
                .map(d -> new PeerNotifySettings(client, d));
    }

    @Override
    public Optional<Integer> getParticipantsCount() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::participantsCount)
                // So far, I have not met such a thing that ChannelMin had a non-null participantsCount
                .or(() -> Optional.ofNullable(minData.participantsCount()));
    }

    @Override
    public Optional<EnumSet<ChatAdminRights>> getAdminRights() {
        return Optional.ofNullable(minData.adminRights()).map(ChatAdminRights::of);
    }

    @Override
    public Optional<ChatBannedRightsSettings> getBannedRights() {
        return Optional.ofNullable(minData.bannedRights()).map(d -> new ChatBannedRightsSettings(client, d));
    }

    @Override
    public Optional<ChatBannedRightsSettings> getDefaultBannedRights() {
        return Optional.ofNullable(minData.defaultBannedRights()).map(d -> new ChatBannedRightsSettings(client, d));
    }

    @Override
    public Optional<Integer> getAvailableMinId() {
        return Optional.ofNullable(fullData).map(ChannelFull::availableMinId);
    }

    @Override
    public Optional<Integer> getFolderId() {
        return Optional.ofNullable(fullData).map(ChannelFull::folderId);
    }

    @Override
    public Optional<List<String>> getPendingSuggestions() {
        return Optional.ofNullable(fullData).map(ChannelFull::pendingSuggestions);
    }

    @Override
    public Optional<Integer> getRequestsPending() {
        return Optional.ofNullable(fullData).map(ChannelFull::requestsPending);
    }

    @Override
    public Optional<Id> getDefaultSendAs() {
        return Optional.ofNullable(fullData).map(ChannelFull::defaultSendAs).map(Id::of);
    }

    @Override
    public Optional<List<Id>> getRecentRequesters() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::recentRequesters)
                .map(list -> list.stream()
                        .map(l -> Id.ofUser(l, null))
                        .collect(Collectors.toList()));
    }

    @Override
    public Optional<List<String>> getAvailableReactions() {
        return Optional.ofNullable(fullData).map(ChannelFull::availableReactions);
    }

    @Override
    public Optional<Integer> getPts() {
        return Optional.ofNullable(fullData).map(ChannelFull::pts);
    }

    @Override
    public Optional<Integer> getReadInboxMaxId() {
        return Optional.ofNullable(fullData).map(ChannelFull::readInboxMaxId);
    }

    @Override
    public Optional<Integer> getReadOutboxMaxId() {
        return Optional.ofNullable(fullData).map(ChannelFull::readOutboxMaxId);
    }

    @Override
    public Optional<Integer> getUnreadCount() {
        return Optional.ofNullable(fullData).map(ChannelFull::unreadCount);
    }

    @Override
    public Optional<Integer> getAdminsCount() {
        return Optional.ofNullable(fullData).map(ChannelFull::adminsCount);
    }

    @Override
    public Optional<Integer> getKickedCount() {
        return Optional.ofNullable(fullData).map(ChannelFull::kickedCount);
    }

    @Override
    public Optional<Integer> getBannedCount() {
        return Optional.ofNullable(fullData).map(ChannelFull::bannedCount);
    }

    @Override
    public Optional<Integer> getOnlineCount() {
        return Optional.ofNullable(fullData).map(ChannelFull::onlineCount);
    }

    @Override
    public Optional<Id> getLinkedChatId() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::linkedChatId)
                .map(Id::ofChat);
    }

    @Override
    public Optional<String> getThemeEmoticon() {
        return Optional.ofNullable(fullData).map(ChannelFull::themeEmoticon);
    }

    @Override
    public Optional<InputGroupCall> getCall() {
        return Optional.ofNullable(fullData).map(ChannelFull::call);
    }

    @Override
    public Optional<Id> getGroupCallDefaultJoinAs() {
        return Optional.ofNullable(fullData)
                .map(ChannelFull::groupcallDefaultJoinAs)
                .map(Id::of);
    }

    @Override
    public Optional<ExportedChatInvite> getExportedInvite() {
        return Optional.ofNullable(exportedChatInvite);
    }

    @Override
    public Optional<Integer> getStatsDcId() {
        return Optional.ofNullable(fullData).map(ChannelFull::statsDc);
    }

    @Override
    public Mono<ChatParticipant> getParticipant(Id participantId) {
        return asInputPeer(participantId)
                .flatMap(participantPeer -> client.getServiceHolder().getChatService().getParticipant(
                ImmutableBaseInputChannel.of(id.asLong(), id.getAccessHash().orElseThrow()),
                participantPeer))
                .map(d -> {
                    // Since ChannelParticipantBanned/ChannelParticipantLeft have Peer typed id field,
                    // then most likely there may be a chat/channel as the participant
                    Peer peerId = TlEntityUtil.getUserId(d.participant());
                    PeerEntity peerEntity;
                    switch (peerId.identifier()) {
                        case PeerChat.ID:
                        case PeerChannel.ID:
                            peerEntity = d.chats().stream()
                                    .filter(u -> TlEntityUtil.isAvailableChat(u) && u.id() == participantId.asLong())
                                    .map(u -> EntityFactory.createChat(client, u, null))
                                    .findFirst()
                                    .orElseThrow();
                            break;
                        case PeerUser.ID:
                            peerEntity = d.users().stream()
                                    .filter(u -> u.identifier() == BaseUser.ID && u.id() == participantId.asLong())
                                    .map(u -> EntityFactory.createUser(client, u))
                                    .findFirst()
                                    .orElseThrow();
                            break;
                        default: throw new IllegalArgumentException("Unknown peer type: " + peerId);
                    }

                    return new ChatParticipant(client, peerEntity, d.participant(), id);
                });
    }

    @Override
    public Flux<ChatParticipant> getParticipants(ChannelParticipantsFilter filter, int offset, int limit) {
        InputChannel channel = TlEntityUtil.toInputChannel(getIdAsPeer());

        return PaginationSupport.paginate(o -> client.getServiceHolder().getChatService()
                .getParticipants(channel, filter, o, limit, 0)
                .cast(BaseChannelParticipants.class), BaseChannelParticipants::count, offset, limit)
                .flatMap(data -> {
                    var chatsMap = data.chats().stream()
                            .filter(TlEntityUtil::isAvailableChat)
                            .map(c -> EntityFactory.createChat(client, c, null))
                            .collect(Collectors.toMap(c -> c.getId().asLong(), Function.identity()));
                    var usersMap = data.users().stream()
                            .filter(u -> u.identifier() == BaseUser.ID)
                            .map(u -> EntityFactory.createUser(client, u))
                            .collect(Collectors.toMap(u -> u.getId().asLong(), Function.identity()));

                    var participants = data.participants().stream()
                            .map(c -> {
                                Peer peerId = TlEntityUtil.getUserId(c);
                                long rawPeerId = TlEntityUtil.getRawPeerId(peerId);
                                PeerEntity peerEntity;
                                switch (peerId.identifier()) {
                                    case PeerChat.ID:
                                    case PeerChannel.ID:
                                        peerEntity = chatsMap.get(rawPeerId);
                                        break;
                                    case PeerUser.ID:
                                        peerEntity = usersMap.get(rawPeerId);
                                        break;
                                    default: throw new IllegalArgumentException("Unknown peer type: " + peerId);
                                }

                                return new ChatParticipant(client, peerEntity, c, id);
                            })
                            .collect(Collectors.toList());

                    return Flux.fromIterable(participants);
                });
    }

    protected Mono<InputPeer> asInputPeer(Id participantId) {
        return Mono.defer(() -> {
            switch (id.getType()) {
                case CHAT: return Mono.just(ImmutableInputPeerChat.of(participantId.asLong()));
                case CHANNEL: return participantId.getAccessHash().isEmpty()
                        ? client.getMtProtoResources().getStoreLayout()
                        .resolveChannel(participantId.asLong()).map(TlEntityUtil::toInputPeer)
                        : Mono.just(ImmutableInputPeerChannel.of(participantId.asLong(),
                        participantId.getAccessHash().orElseThrow()));
                case USER: return participantId.getAccessHash().isEmpty()
                        ? client.getMtProtoResources().getStoreLayout()
                        .resolveUser(participantId.asLong()).map(TlEntityUtil::toInputPeer)
                        : Mono.just(ImmutableInputPeerUser.of(participantId.asLong(),
                        participantId.getAccessHash().orElseThrow()));
                default: throw new IllegalStateException();
            }
        });
    }
}
