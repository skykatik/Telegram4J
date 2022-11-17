package telegram4j.core;

import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.tcp.TcpClient;
import reactor.scheduler.forkjoin.ForkJoinPoolScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import telegram4j.core.event.DefaultEventDispatcher;
import telegram4j.core.event.DefaultUpdatesManager;
import telegram4j.core.event.DefaultUpdatesManager.Options;
import telegram4j.core.event.EventDispatcher;
import telegram4j.core.event.UpdatesManager;
import telegram4j.core.event.dispatcher.UpdatesMapper;
import telegram4j.core.event.domain.Event;
import telegram4j.core.retriever.EntityRetrievalStrategy;
import telegram4j.core.retriever.EntityRetriever;
import telegram4j.core.util.Id;
import telegram4j.core.util.UnavailableChatPolicy;
import telegram4j.core.util.parser.EntityParserFactory;
import telegram4j.mtproto.*;
import telegram4j.mtproto.service.ServiceHolder;
import telegram4j.mtproto.store.StoreLayout;
import telegram4j.mtproto.store.StoreLayoutImpl;
import telegram4j.mtproto.transport.IntermediateTransport;
import telegram4j.mtproto.transport.Transport;
import telegram4j.mtproto.util.EmissionHandlers;
import telegram4j.tl.BaseUser;
import telegram4j.tl.InputUserSelf;
import telegram4j.tl.TlInfo;
import telegram4j.tl.api.TlMethod;
import telegram4j.tl.auth.BaseAuthorization;
import telegram4j.tl.request.InitConnection;
import telegram4j.tl.request.InvokeWithLayer;
import telegram4j.tl.request.auth.ImmutableImportBotAuthorization;
import telegram4j.tl.request.help.GetConfig;
import telegram4j.tl.request.updates.GetState;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

public final class MTProtoBootstrap<O extends MTProtoOptions> {

    private static final boolean parseBotIdFromToken = Boolean.getBoolean("telegram4j.core.parseBotIdFromToken");
    private static final Logger log = Loggers.getLogger(MTProtoBootstrap.class);

    private final Function<MTProtoOptions, ? extends O> optionsModifier;
    private final AuthorizationResources authResources;
    private final List<ResponseTransformer> responseTransformers = new ArrayList<>();

    private TcpClient tcpClient;
    private Supplier<Transport> transport = () -> new IntermediateTransport(true);
    private RetryBackoffSpec connectionRetry;
    private RetryBackoffSpec authRetry;

    @Nullable
    private EntityParserFactory defaultEntityParserFactory;
    private EntityRetrievalStrategy entityRetrievalStrategy = EntityRetrievalStrategy.STORE_FALLBACK_RPC;
    private Function<MTProtoTelegramClient, UpdatesManager> updatesManagerFactory = c ->
            new DefaultUpdatesManager(c, new Options(c));
    private Function<MTProtoClientGroupOptions, MTProtoClientGroup> clientGroupFactory = options ->
            new DefaultMTProtoClientGroup(options, new DefaultMTProtoClientGroup.Options());
    private UnavailableChatPolicy unavailableChatPolicy = UnavailableChatPolicy.NULL_MAPPING;
    private PublicRsaKeyRegister publicRsaKeyRegister;
    private DcOptions dcOptions;
    private InitConnectionParams initConnectionParams;
    private StoreLayout storeLayout;
    private EventDispatcher eventDispatcher;
    private DataCenter dataCenter;

    MTProtoBootstrap(Function<MTProtoOptions, ? extends O> optionsModifier, AuthorizationResources authResources) {
        this.optionsModifier = optionsModifier;
        this.authResources = authResources;
    }

    /**
     * Creates new {@code MTProtoBootstrap} with new option modifier step.
     *
     * @param optionsModifier A new option mapper for composing.
     * @param <O1> A new type of options.
     * @return This new builder with new option modifier.
     */
    public <O1 extends MTProtoOptions> MTProtoBootstrap<O1> setExtraOptions(Function<? super O, ? extends O1> optionsModifier) {
        return new MTProtoBootstrap<>(this.optionsModifier.andThen(optionsModifier), authResources);
    }

    /**
     * Sets the factory of client group for working with different datacenters and sessions.
     * <p>
     * If custom implementation doesn't set, {@link DefaultMTProtoClientGroup} will be used.
     *
     * @param clientGroupFactory A new factory for client group.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setClientGroupManager(Function<MTProtoClientGroupOptions, MTProtoClientGroup> clientGroupFactory) {
        this.clientGroupFactory = Objects.requireNonNull(clientGroupFactory);
        return this;
    }

    /**
     * Sets store layout for accessing and persisting incoming data from Telegram API.
     * <p>
     * If custom implementation doesn't set, {@link StoreLayoutImpl} with message LRU cache bounded to {@literal 10000} will be used.
     *
     * @param storeLayout A new store layout implementation for client.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setStoreLayout(StoreLayout storeLayout) {
        this.storeLayout = Objects.requireNonNull(storeLayout);
        return this;
    }

    /**
     * Sets TCP transport factory for all MTProto clients.
     * <p>
     * If custom transport factory doesn't set, {@link IntermediateTransport} factory will be used as threshold.
     *
     * @see <a href="https://core.telegram.org/mtproto/mtproto-transports">MTProto Transport</a>
     * @param transport A new {@link Transport} factory for clients.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setTransport(Supplier<Transport> transport) {
        this.transport = Objects.requireNonNull(transport);
        return this;
    }

    /**
     * Sets netty's TCP client for all MTProto clients.
     * <p>
     * If custom client doesn't set, {@link TcpClient#create() pooled} implementation will be used.
     *
     * @param tcpClient A new netty's {@link TcpClient} for MTProto clients.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setTcpClient(TcpClient tcpClient) {
        this.tcpClient = Objects.requireNonNull(tcpClient);
        return this;
    }

    /**
     * Sets connection identity parameters.
     * That parameters send on connection establishment, i.e. sending {@link InitConnection} request.
     * <p>
     * If custom parameters doesn't set, {@link InitConnectionParams#getDefault()} will be used.
     *
     * @param initConnectionParams A new connection identity parameters.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setInitConnectionParams(InitConnectionParams initConnectionParams) {
        this.initConnectionParams = Objects.requireNonNull(initConnectionParams);
        return this;
    }

    /**
     * Sets custom {@link EventDispatcher} implementation for distributing mapped {@link Event events} to subscribers.
     * <p>
     * If custom event dispatcher doesn't set, {@link Sinks sinks}-based {@link DefaultEventDispatcher} implementation will be used.
     *
     * @param eventDispatcher A new event dispatcher.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setEventDispatcher(EventDispatcher eventDispatcher) {
        this.eventDispatcher = Objects.requireNonNull(eventDispatcher);
        return this;
    }

    /**
     * Sets DC address for main MTProto client.
     * <p>
     * If DC address doesn't set, production IPv4 DC 2 (europe) will be used.
     *
     * @param dataCenter A new DC address to use.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setDataCenter(DataCenter dataCenter) {
        this.dataCenter = Objects.requireNonNull(dataCenter);
        return this;
    }

    /**
     * Sets updates manager factory for creating updates manager.
     * <p>
     * If custom updates manager factory doesn't set, {@link UpdatesMapper} will be used.
     *
     * @param updatesManagerFactory A new factory for creating {@link UpdatesManager}.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setUpdatesManager(Function<MTProtoTelegramClient, UpdatesManager> updatesManagerFactory) {
        this.updatesManagerFactory = Objects.requireNonNull(updatesManagerFactory);
        return this;
    }

    /**
     * Sets retry strategy for mtproto client reconnection.
     * <p>
     * If custom doesn't set, {@code Retry.fixedDelay(Integer.MAX_VALUE, Duration.ofSeconds(5))} will be used.
     *
     * @param connectionRetry A new retry strategy for mtproto client reconnection.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setConnectionRetry(RetryBackoffSpec connectionRetry) {
        this.connectionRetry = Objects.requireNonNull(connectionRetry);
        return this;
    }

    /**
     * Sets retry strategy for auth key generation.
     * <p>
     * If custom doesn't set, {@code Retry.fixedDelay(5, Duration.ofSeconds(3))} will be used.
     *
     * @param authRetry A new retry strategy for auth key generation.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setAuthRetry(RetryBackoffSpec authRetry) {
        this.authRetry = Objects.requireNonNull(authRetry);
        return this;
    }

    /**
     * Sets entity retrieval strategy factory for creating default entity retriever.
     * <p>
     * If custom entity retrieval strategy doesn't set, {@link EntityRetrievalStrategy#STORE_FALLBACK_RPC} will be used.
     *
     * @param strategy A new default strategy for creating {@link EntityRetriever}.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setEntityRetrieverStrategy(EntityRetrievalStrategy strategy) {
        this.entityRetrievalStrategy = Objects.requireNonNull(strategy);
        return this;
    }

    /**
     * Sets handle policy for unavailable chats and channels.
     * <p>
     * By default, {@link UnavailableChatPolicy#NULL_MAPPING} will be used.
     *
     * @param policy A new policy for unavailable chats and channels.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setUnavailableChatPolicy(UnavailableChatPolicy policy) {
        this.unavailableChatPolicy = Objects.requireNonNull(policy);
        return this;
    }

    /**
     * Sets default global {@link EntityParserFactory} for text parsing, by default is {@code null}.
     *
     * @param defaultEntityParserFactory A new default {@link EntityParserFactory} for text parsing.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setDefaultEntityParserFactory(EntityParserFactory defaultEntityParserFactory) {
        this.defaultEntityParserFactory = Objects.requireNonNull(defaultEntityParserFactory);
        return this;
    }

    /**
     * Sets register with known public RSA keys, needed for auth key generation,
     * by default {@link PublicRsaKeyRegister#createDefault()} will be used.
     *
     * @param publicRsaKeyRegister A new register with known public RSA keys.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setPublicRsaKeyRegister(PublicRsaKeyRegister publicRsaKeyRegister) {
        this.publicRsaKeyRegister = Objects.requireNonNull(publicRsaKeyRegister);
        return this;
    }

    /**
     * Sets list of known dc options, used in connection establishment,
     * by default {@link DcOptions#createDefault(boolean, boolean)} will be used.
     *
     * @param dcOptions A new list of known dc options.
     * @return This builder.
     */
    public MTProtoBootstrap<O> setDcOptions(DcOptions dcOptions) {
        this.dcOptions = Objects.requireNonNull(dcOptions);
        return this;
    }

    /**
     * Adds new {@link ResponseTransformer} to transformation list.
     *
     * @param responseTransformer The new {@link ResponseTransformer} to add.
     * @return This builder.
     */
    public MTProtoBootstrap<O> addResponseTransformer(ResponseTransformer responseTransformer) {
        Objects.requireNonNull(responseTransformer);
        responseTransformers.add(responseTransformer);
        return this;
    }

    /**
     * Prepare and connect {@link DefaultMTProtoClient} to the specified Telegram DC and
     * on successfully completion emit {@link MTProtoTelegramClient} to subscribers.
     * Any errors caused on connection time will be emitted to a {@link Mono} and terminate client with disconnecting.
     *
     * @param func A function to use client until it's disconnected.
     * @see #connect()
     * @return A {@link Mono} that upon subscription and successfully completion emits a {@link MTProtoTelegramClient}.
     */
    public Mono<Void> withConnection(Function<MTProtoTelegramClient, ? extends Publisher<?>> func) {
        return Mono.usingWhen(connect(), client -> Flux.from(func.apply(client)).then(client.onDisconnect()),
                MTProtoTelegramClient::disconnect);
    }

    /**
     * Prepare and connect {@link DefaultMTProtoClient} to the specified Telegram DC and
     * on successfully completion emit {@link MTProtoTelegramClient} to subscribers.
     * Any errors caused on connection time will be emitted to a {@link Mono}.
     *
     * @return A {@link Mono} that upon subscription and successfully completion emits a {@link MTProtoTelegramClient}.
     */
    public Mono<MTProtoTelegramClient> connect() {
        return connect(DefaultMTProtoClient::new);
    }

    /**
     * Prepare and connect MTProto client to the specified Telegram DC and
     * on successfully completion emit {@link MTProtoTelegramClient} to subscribers.
     * Any errors caused on connection time will be emitted to a {@link Mono}.
     *
     * @param clientFactory A new factory for constructing main MTProto client.
     * @return A {@link Mono} that upon subscription and successfully completion emits a {@link MTProtoTelegramClient}.
     */
    public Mono<MTProtoTelegramClient> connect(Function<? super O, ? extends MainMTProtoClient> clientFactory) {
        return Mono.create(sink -> {
            StoreLayout storeLayout = initStoreLayout();
            EventDispatcher eventDispatcher = initEventDispatcher();
            Sinks.Empty<Void> onDisconnect = Sinks.empty();

            var invokeWithLayout =
                    InvokeWithLayer.<Object, InitConnection<Object, TlMethod<?>>>builder()
                            .layer(TlInfo.LAYER)
                            .query(initConnection())
                            .build();

            var dcOptions = initDcOptions();
            var pubRsaKeyRegister = initPublicRsaKeyRegister();
            MainMTProtoClient leadClient = clientFactory.apply(optionsModifier.apply(
                    new MTProtoOptions(initDataCenter(dcOptions), initTcpClient(),
                            pubRsaKeyRegister, transport,
                            storeLayout, EmissionHandlers.DEFAULT_PARKING,
                            initConnectionRetry(), initAuthRetry(),
                            List.copyOf(responseTransformers), invokeWithLayout)));
            MTProtoClientGroup clientGroupManager = clientGroupFactory.apply(
                    new MTProtoClientGroupOptions(leadClient, storeLayout, dcOptions));

            MTProtoResources mtProtoResources = new MTProtoResources(storeLayout, eventDispatcher,
                    defaultEntityParserFactory, unavailableChatPolicy);
            ServiceHolder serviceHolder = new ServiceHolder(clientGroupManager, storeLayout);

            Id[] selfId = {null};
            MTProtoTelegramClient telegramClient = new MTProtoTelegramClient(
                    authResources, clientGroupManager,
                    mtProtoResources, updatesManagerFactory, selfId,
                    serviceHolder, entityRetrievalStrategy, onDisconnect.asMono());

            Runnable disconnect = () -> {
                eventDispatcher.shutdown();
                telegramClient.getUpdatesManager().shutdown();
                onDisconnect.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
                log.info("MTProto client disconnected");
            };

            Disposable.Composite composite = Disposables.composite();

            composite.add(storeLayout.initialize()
                    .subscribe(null, t -> log.error("Store layout terminated with an error", t),
                            () -> log.debug("Store layout completed")));
            composite.add(storeLayout.updatePublicRsaKeyRegister(pubRsaKeyRegister).subscribe());

            composite.add(clientGroupManager.start()
                    .takeUntilOther(onDisconnect.asMono())
                    .doOnError(sink::error)
                    .subscribe(null, t -> log.error("MTProto client group terminated with an error", t),
                            () -> log.debug("MTProto client group completed")));

            composite.add(leadClient.connect()
                    .doOnError(sink::error)
                    .subscribe(null, t -> log.error("MTProto client terminated with an error", t)));

            composite.add(leadClient.updates().asFlux()
                    .takeUntilOther(onDisconnect.asMono())
                    .flatMap(telegramClient.getUpdatesManager()::handle)
                    .doOnNext(eventDispatcher::publish)
                    .subscribe(null, t -> log.error("Event dispatcher terminated with an error", t),
                            () -> log.debug("Event dispatcher completed")));

            composite.add(telegramClient.getUpdatesManager().start()
                    .takeUntilOther(onDisconnect.asMono())
                    .subscribe(null, t -> log.error("Updates manager terminated with an error", t),
                            () -> log.debug("Updates manager completed")));

            AtomicBoolean emit = new AtomicBoolean(true);

            composite.add(leadClient.state()
                    .takeUntilOther(onDisconnect.asMono())
                    .flatMap(state -> {
                        switch (state) {
                            case CLOSED: return Mono.fromRunnable(disconnect);
                            case READY:
                                Mono<Void> fetchSelfId = Mono.defer(() -> {
                                            // bot user id writes before ':' char
                                            if (parseBotIdFromToken && authResources.isBot()) {
                                                return Mono.fromSupplier(() -> Id.ofUser(authResources.getBotAuthToken()
                                                        .map(t -> Long.parseLong(t.split(":", 2)[0]))
                                                        .orElseThrow()));
                                            }
                                            return storeLayout.getSelfId().map(Id::ofUser);
                                        })
                                        .switchIfEmpty(serviceHolder.getUserService()
                                                .getFullUser(InputUserSelf.instance())
                                                .map(user -> {
                                                    var self = (BaseUser) user.users().get(0);
                                                    long ac = Objects.requireNonNull(self.accessHash());
                                                    return Id.ofUser(user.fullUser().id(), ac);
                                                }))
                                        .doOnNext(id -> selfId[0] = id)
                                        .then();

                                return Mono.defer(() -> {
                                    if (authResources.isBot()) {
                                        return leadClient.sendAwait(ImmutableImportBotAuthorization.of(0,
                                                authResources.getApiId(), authResources.getApiHash(),
                                                authResources.getBotAuthToken().orElseThrow()))
                                                .cast(BaseAuthorization.class)
                                                .flatMap(storeLayout::onAuthorization);

                                    }
                                    // to trigger user auth
                                    return leadClient.sendAwait(GetState.instance())
                                            .retryWhen(Retry.indefinitely()
                                                    .filter(RpcException.isErrorCode(401)
                                                            .and(t -> !authResources.isBot()))
                                                    // delegate all auth work to the user and trigger authorization only if auth key is new
                                                    .doBeforeRetryAsync(signal -> Mono.justOrEmpty(authResources.getAuthHandler())
                                                            .flatMap(f -> f.apply(telegramClient))
                                                            .flatMap(storeLayout::onAuthorization)
                                                            .then()));
                                })
                                // startup errors must close client
                                .onErrorResume(e -> leadClient.close()
                                        .then(onDisconnect.asMono())
                                        .then(Mono.fromRunnable(() -> sink.error(e))))
                                .flatMap(res -> fetchSelfId.then(telegramClient.getUpdatesManager().fillGap()))
                                .doOnSuccess(any -> {
                                    if (emit.compareAndSet(true, false)) {
                                        sink.success(telegramClient);
                                    }
                                });
                            default:
                                return Mono.empty();
                        }
                    })
                    .subscribe(null, t -> log.error("State handler terminated with an error", t),
                            () -> log.debug("State handler completed")));

            sink.onCancel(composite);
        });
    }

    // Resources initialization
    // ==========================

    private InitConnection<Object, TlMethod<?>> initConnection() {
        InitConnectionParams params = initConnectionParams != null
                ? initConnectionParams
                : InitConnectionParams.getDefault();

        var initConnection = InitConnection.builder()
                .apiId(authResources.getApiId())
                .appVersion(params.getAppVersion())
                .deviceModel(params.getDeviceModel())
                .langCode(params.getLangCode())
                .langPack(params.getLangPack())
                .systemVersion(params.getSystemVersion())
                .systemLangCode(params.getSystemLangCode())
                .query(GetConfig.instance());

        params.getProxy().ifPresent(initConnection::proxy);
        params.getParams().ifPresent(initConnection::params);

        return initConnection.build();
    }

    private TcpClient initTcpClient() {
        if (tcpClient != null) {
            return tcpClient;
        }
        return TcpClient.create();
    }

    private EventDispatcher initEventDispatcher() {
        if (eventDispatcher != null) {
            return eventDispatcher;
        }
        return new DefaultEventDispatcher(ForkJoinPoolScheduler.create("t4j-events", 4),
                Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false),
                EmissionHandlers.DEFAULT_PARKING);
    }

    private StoreLayout initStoreLayout() {
        if (storeLayout != null) {
            return storeLayout;
        }
        return new StoreLayoutImpl(c -> c.maximumSize(1000));
    }

    private DataCenter initDataCenter(DcOptions opts) {
        if (dataCenter != null) {
            return dataCenter;
        }
        return opts.find(DataCenter.Type.REGULAR, 2)
                .orElseThrow(() -> new IllegalStateException("Could not find dc 2 for main client in options: " + opts));
    }

    private DcOptions initDcOptions() {
        if (dcOptions != null) {
            return dcOptions;
        }
        return DcOptions.createDefault(false, false);
    }

    private RetryBackoffSpec initConnectionRetry() {
        if (connectionRetry != null) {
            return connectionRetry;
        }
        return Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(5));
    }

    private PublicRsaKeyRegister initPublicRsaKeyRegister() {
        if (publicRsaKeyRegister != null) {
            return publicRsaKeyRegister;
        }
        return PublicRsaKeyRegister.createDefault();
    }

    private RetryBackoffSpec initAuthRetry() {
        if (authRetry != null) {
            return authRetry;
        }
        return Retry.fixedDelay(5, Duration.ofSeconds(3));
    }
}
