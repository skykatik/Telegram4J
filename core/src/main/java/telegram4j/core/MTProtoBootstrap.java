package telegram4j.core;

import org.reactivestreams.Publisher;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.function.TupleUtils;
import reactor.scheduler.forkjoin.ForkJoinPoolScheduler;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.annotation.Nullable;
import reactor.util.concurrent.Queues;
import reactor.util.retry.Retry;
import reactor.util.retry.RetryBackoffSpec;
import telegram4j.core.event.DefaultEventDispatcher;
import telegram4j.core.event.DefaultUpdatesManager;
import telegram4j.core.event.EventDispatcher;
import telegram4j.core.event.UpdatesManager;
import telegram4j.core.event.dispatcher.UpdatesMapper;
import telegram4j.core.event.domain.Event;
import telegram4j.core.internal.Preconditions;
import telegram4j.core.retriever.EntityRetrievalStrategy;
import telegram4j.core.retriever.EntityRetriever;
import telegram4j.core.util.Id;
import telegram4j.core.util.UnavailableChatPolicy;
import telegram4j.core.util.parser.EntityParserFactory;
import telegram4j.mtproto.*;
import telegram4j.mtproto.auth.DhPrimeChecker;
import telegram4j.mtproto.auth.DhPrimeCheckerCache;
import telegram4j.mtproto.client.*;
import telegram4j.mtproto.resource.TcpClientResources;
import telegram4j.mtproto.service.ServiceHolder;
import telegram4j.mtproto.store.FileStoreLayout;
import telegram4j.mtproto.store.StoreLayout;
import telegram4j.mtproto.store.StoreLayoutImpl;
import telegram4j.mtproto.transport.IntermediateTransport;
import telegram4j.mtproto.transport.Transport;
import telegram4j.mtproto.transport.TransportFactory;
import telegram4j.tl.BaseUser;
import telegram4j.tl.InputUserSelf;
import telegram4j.tl.TlInfo;
import telegram4j.tl.api.TlMethod;
import telegram4j.tl.auth.Authorization;
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

public final class MTProtoBootstrap {

    private static final boolean parseBotIdFromToken = Boolean.getBoolean("telegram4j.core.parseBotIdFromToken");
    private static final Logger log = Loggers.getLogger(MTProtoBootstrap.class);

    private final AuthorizationResources authResources;
    @Nullable
    private final AuthorisationHandler authHandler;
    private final List<ResponseTransformer> responseTransformers = new ArrayList<>();

    private TransportFactory transportFactory = dc -> new IntermediateTransport(true);
    private Function<MTProtoOptions, ClientFactory> clientFactory = DefaultClientFactory::new;
    private RetryBackoffSpec connectionRetry;
    private RetryBackoffSpec authRetry;

    @Nullable
    private EntityParserFactory defaultEntityParserFactory;
    private EntityRetrievalStrategy entityRetrievalStrategy = EntityRetrievalStrategy.STORE_FALLBACK_RPC;
    private Function<MTProtoTelegramClient, UpdatesManager> updatesManagerFactory = c ->
            new DefaultUpdatesManager(c, new DefaultUpdatesManager.Options(c));
    private Function<MTProtoClientGroupOptions, MTProtoClientGroup> clientGroupFactory = options ->
            new DefaultMTProtoClientGroup(new DefaultMTProtoClientGroup.Options(options));
    private UnavailableChatPolicy unavailableChatPolicy = UnavailableChatPolicy.NULL_MAPPING;
    private DhPrimeChecker dhPrimeChecker;
    private PublicRsaKeyRegister publicRsaKeyRegister;
    private DcOptions dcOptions;
    private InitConnectionParams initConnectionParams;
    private StoreLayout storeLayout;
    private EventDispatcher eventDispatcher;
    private DataCenter dataCenter;
    private int gzipWrappingSizeThreshold = 16 * 1024;
    private TcpClientResources tcpClientResources;
    private UpdateDispatcher updateDispatcher;

    MTProtoBootstrap(AuthorizationResources authResources, @Nullable AuthorisationHandler authHandler) {
        this.authResources = authResources;
        this.authHandler = authHandler;
    }

    /**
     * Sets the factory of client group for working with different datacenters and sessions.
     * <p>
     * If custom implementation doesn't set, {@link DefaultMTProtoClientGroup} will be used.
     *
     * @param clientGroupFactory A new factory for client group.
     * @return This builder.
     */
    public MTProtoBootstrap setClientGroupManager(Function<MTProtoClientGroupOptions, MTProtoClientGroup> clientGroupFactory) {
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
    public MTProtoBootstrap setStoreLayout(StoreLayout storeLayout) {
        this.storeLayout = Objects.requireNonNull(storeLayout);
        return this;
    }

    /**
     * Sets TCP transport factory for all MTProto clients.
     * <p>
     * If custom transport factory doesn't set, {@link IntermediateTransport} factory will be used as threshold.
     *
     * @param transportFactory A new {@link Transport} factory for clients.
     * @return This builder.
     * @see <a href="https://core.telegram.org/mtproto/mtproto-transports">MTProto Transport</a>
     */
    public MTProtoBootstrap setTransportFactory(TransportFactory transportFactory) {
        this.transportFactory = Objects.requireNonNull(transportFactory);
        return this;
    }

    // TODO docs
    public MTProtoBootstrap setTcpClientResources(TcpClientResources tcpClientResources) {
        this.tcpClientResources = Objects.requireNonNull(tcpClientResources);
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
    public MTProtoBootstrap setInitConnectionParams(InitConnectionParams initConnectionParams) {
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
    public MTProtoBootstrap setEventDispatcher(EventDispatcher eventDispatcher) {
        this.eventDispatcher = Objects.requireNonNull(eventDispatcher);
        return this;
    }

    // TODO docs
    public MTProtoBootstrap setUpdateDispatcher(UpdateDispatcher updateDispatcher) {
        this.updateDispatcher = Objects.requireNonNull(updateDispatcher);
        return this;
    }

    /**
     * Sets default DC address for main MTProto client.
     *
     * <p> If DC address doesn't set, production IPv4 DC 2 (europe) will be used.
     * This DC will be used only if local store have no information.
     *
     * @param dataCenter A new DC address to use.
     * @return This builder.
     * @throws IllegalArgumentException if type of specified option is not {@link DataCenter.Type#REGULAR}.
     */
    public MTProtoBootstrap setDataCenter(DataCenter dataCenter) {
        Preconditions.requireArgument(dataCenter.getType() == DataCenter.Type.REGULAR, "Invalid type for main DC");
        this.dataCenter = dataCenter;
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
    public MTProtoBootstrap setUpdatesManager(Function<MTProtoTelegramClient, UpdatesManager> updatesManagerFactory) {
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
    public MTProtoBootstrap setConnectionRetry(RetryBackoffSpec connectionRetry) {
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
    public MTProtoBootstrap setAuthRetry(RetryBackoffSpec authRetry) {
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
    public MTProtoBootstrap setEntityRetrieverStrategy(EntityRetrievalStrategy strategy) {
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
    public MTProtoBootstrap setUnavailableChatPolicy(UnavailableChatPolicy policy) {
        this.unavailableChatPolicy = Objects.requireNonNull(policy);
        return this;
    }

    /**
     * Sets default global {@link EntityParserFactory} for text parsing, by default is {@code null}.
     *
     * @param defaultEntityParserFactory A new default {@link EntityParserFactory} for text parsing.
     * @return This builder.
     */
    public MTProtoBootstrap setDefaultEntityParserFactory(EntityParserFactory defaultEntityParserFactory) {
        this.defaultEntityParserFactory = Objects.requireNonNull(defaultEntityParserFactory);
        return this;
    }

    /**
     * Sets register with known public RSA keys, needed for auth key generation,
     * by default {@link PublicRsaKeyRegister#createDefault()} will be used.
     *
     * <p> This register will be used only if {@link StoreLayout} have no keys.
     *
     * @param publicRsaKeyRegister A new register with known public RSA keys.
     * @return This builder.
     */
    public MTProtoBootstrap setPublicRsaKeyRegister(PublicRsaKeyRegister publicRsaKeyRegister) {
        this.publicRsaKeyRegister = Objects.requireNonNull(publicRsaKeyRegister);
        return this;
    }

    /**
     * Sets DH prime register with known primes, needed for auth key generation,
     * by default the common {@link DhPrimeCheckerCache#instance()} will be used.
     *
     * @param dhPrimeChecker A new prime checker.
     * @return This builder.
     */
    public MTProtoBootstrap setDhPrimeChecker(DhPrimeChecker dhPrimeChecker) {
        this.dhPrimeChecker = Objects.requireNonNull(dhPrimeChecker);
        return this;
    }

    /**
     * Sets list of known dc options, used in connection establishment,
     * by default {@link DcOptions#createDefault(boolean, boolean)} will be used.
     *
     * <p> This options will be used only if {@link StoreLayout} have no options.
     *
     * @param dcOptions A new list of known dc options.
     * @return This builder.
     */
    public MTProtoBootstrap setDcOptions(DcOptions dcOptions) {
        this.dcOptions = Objects.requireNonNull(dcOptions);
        return this;
    }

    /**
     * Adds new {@link ResponseTransformer} to transformation list.
     *
     * @param responseTransformer The new {@link ResponseTransformer} to add.
     * @return This builder.
     */
    public MTProtoBootstrap addResponseTransformer(ResponseTransformer responseTransformer) {
        responseTransformers.add(Objects.requireNonNull(responseTransformer));
        return this;
    }

    /**
     * Sets size threshold for gzip packing mtproto queries, by default equals to 16KB.
     *
     * @throws IllegalArgumentException if {@code gzipWrappingSizeThreshold} is negative.
     * @param gzipWrappingSizeThreshold The new request's size threshold.
     * @return This builder.
     */
    public MTProtoBootstrap setGzipWrappingSizeThreshold(int gzipWrappingSizeThreshold) {
        Preconditions.requireArgument(gzipWrappingSizeThreshold > 0, "Invalid threshold value");
        this.gzipWrappingSizeThreshold = gzipWrappingSizeThreshold;
        return this;
    }

    /**
     * Sets client factory for creating mtproto clients, by default {@link DefaultClientFactory} is used.
     *
     * @param clientFactory The new client factory constructor.
     * @return This builder.
     */
    public MTProtoBootstrap setClientFactory(Function<MTProtoOptions, ClientFactory> clientFactory) {
        this.clientFactory = Objects.requireNonNull(clientFactory);
        return this;
    }

    /**
     * Prepare and connect {@link MTProtoClient} to the specified Telegram DC and
     * on successfully completion emit {@link MTProtoTelegramClient} to subscribers.
     * Any errors caused on connection time will be emitted to a {@link Mono} and terminate client with disconnecting.
     *
     * @param func A function to use client until it's disconnected.
     * @return A {@link Mono} that upon subscription and successfully completion emits a {@link MTProtoTelegramClient}.
     * @see #connect()
     */
    public Mono<Void> withConnection(Function<MTProtoTelegramClient, ? extends Publisher<?>> func) {
        return Mono.usingWhen(connect(), client -> Flux.from(func.apply(client)).then(client.onDisconnect()),
                MTProtoTelegramClient::disconnect);
    }

    /**
     * Prepare and connect MTProto client to the specified Telegram DC and
     * on successfully completion emit {@link MTProtoTelegramClient} to subscribers.
     * Any errors caused on connection time will be emitted to a {@link Mono}.
     *
     * @return A {@link Mono} that upon subscription and successfully completion emits a {@link MTProtoTelegramClient}.
     */
    public Mono<MTProtoTelegramClient> connect() {
        return Mono.create(sink -> {
            StoreLayout storeLayout = initStoreLayout();

            var composite = Disposables.composite();
            composite.add(storeLayout.initialize()
                    // Here the subscription order is important
                    // and therefore need to make sure that initialization blocks
                    .subscribeOn(Schedulers.immediate())
                    .subscribe(null, t -> log.error("Store layout terminated with an error", t)));

            var loadDcOptions = storeLayout.getDcOptions()
                    .switchIfEmpty(Mono.defer(() -> { // no DcOptions present in store - use default
                        var dcOptions = initDcOptions();
                        return storeLayout.updateDcOptions(dcOptions)
                                .thenReturn(dcOptions);
                    }));
            var loadMainDc = loadDcOptions
                    .zipWhen(dcOptions -> storeLayout.getDataCenter()
                            .switchIfEmpty(Mono.fromSupplier(() -> initDataCenter(dcOptions))));
            composite.add(loadMainDc
                    .flatMap(TupleUtils.function((dcOptions, mainDc) -> {
                        var options = new MTProtoOptions(
                                initTcpClientResources(), publicRsaKeyRegister,
                                dhPrimeChecker, transportFactory, storeLayout, initConnectionRetry(), initAuthRetry(),
                                List.copyOf(responseTransformers),
                                InvokeWithLayer.<Object, InitConnection<Object, TlMethod<?>>>builder()
                                        .layer(TlInfo.LAYER)
                                        .query(initConnection())
                                        .build(), gzipWrappingSizeThreshold);

                        ClientFactory clientFactory = this.clientFactory.apply(options);
                        MTProtoClientGroup clientGroup = clientGroupFactory.apply(
                                new MTProtoClientGroupOptions(mainDc, clientFactory,
                                        storeLayout, initUpdateDispatcher()));

                        return tryConnect(clientGroup, storeLayout, dcOptions)
                                .flatMap(ignored -> initializeClient(clientGroup, storeLayout));
                    }))
                    .subscribe(sink::success, sink::error));

            sink.onCancel(composite);
        });
    }

    private Mono<MTProtoClientGroup> tryConnect(MTProtoClientGroup clientGroup,
                                                StoreLayout storeLayout, DcOptions dcOptions) {
        return Mono.create(sink -> {
            var mainClient = clientGroup.main();

            sink.onCancel(mainClient.connect()
                    .onErrorResume(e -> clientGroup.close()
                            .then(Mono.fromRunnable(() -> sink.error(e)))
                            .then(Mono.never()))
                    .then(Mono.defer(() -> {
                        if (authHandler != null) {
                            // to trigger user auth
                            return mainClient.sendAwait(GetState.instance())
                                    .doOnNext(ign -> sink.success(clientGroup))
                                    .onErrorResume(RpcException.isErrorCode(401), t ->
                                            authHandler.process(clientGroup, storeLayout, authResources)
                                            // users can emit empty signals if they want to gracefully destroy the client
                                            .switchIfEmpty(Mono.defer(clientGroup::close)
                                                    .then(Mono.fromRunnable(sink::success)))
                                            .flatMap(auth -> storeLayout.onAuthorization(auth)
                                                    .doOnSuccess(ign -> sink.success(clientGroup)))
                                            .then(Mono.empty()));
                        }
                        return mainClient.sendAwait(ImmutableImportBotAuthorization.of(0,
                                        authResources.getApiId(), authResources.getApiHash(),
                                        authResources.getBotAuthToken().orElseThrow()))
                                .onErrorResume(RpcException.isErrorCode(303),
                                        e -> redirectToDc((RpcException) e, mainClient,
                                                clientGroup, storeLayout, dcOptions))
                                .cast(BaseAuthorization.class)
                                .flatMap(auth -> storeLayout.onAuthorization(auth)
                                        .doOnSuccess(ign -> sink.success(clientGroup)));
                    }))
                    .onErrorResume(e -> clientGroup.close()
                            .then(Mono.fromRunnable(() -> sink.error(e))))
                    .subscribe());
        });
    }

    private Mono<MTProtoTelegramClient> initializeClient(MTProtoClientGroup clientGroup, StoreLayout storeLayout) {
        return Mono.create(sink -> {
            EventDispatcher eventDispatcher = initEventDispatcher();
            Sinks.Empty<Void> onDisconnect = Sinks.empty();

            MTProtoResources mtProtoResources = new MTProtoResources(storeLayout, eventDispatcher,
                    defaultEntityParserFactory, unavailableChatPolicy);
            ServiceHolder serviceHolder = new ServiceHolder(clientGroup, storeLayout);

            Id[] selfId = {null};
            MTProtoTelegramClient telegramClient = new MTProtoTelegramClient(
                    authResources, clientGroup,
                    mtProtoResources, updatesManagerFactory, selfId,
                    serviceHolder, entityRetrievalStrategy, onDisconnect.asMono());

            Runnable disconnect = () -> {
                eventDispatcher.shutdown();
                telegramClient.getUpdatesManager().shutdown();
                onDisconnect.emitEmpty(Sinks.EmitFailureHandler.FAIL_FAST);
            };

            var composite = Disposables.composite();

            composite.add(clientGroup.start()
                    .takeUntilOther(onDisconnect.asMono())
                    .doOnError(sink::error)
                    .subscribe(null, t -> log.error("MTProto client group terminated with an error", t),
                            () -> log.debug("MTProto client group completed")));

            composite.add(clientGroup.updates().all()
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
            composite.add(clientGroup.main().state()
                    .takeUntilOther(onDisconnect.asMono())
                    .flatMap(state -> switch (state) {
                        case CLOSED -> Mono.fromRunnable(disconnect);
                        case READY -> Mono.defer(() -> {
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
                                .doOnNext(id -> {
                                    if (emit.compareAndSet(true, false)) {
                                        selfId[0] = id;
                                        sink.success(telegramClient);
                                    }
                                })
                                .then(telegramClient.getUpdatesManager().fillGap());
                        default -> Mono.empty();
                    })
                    .subscribe(null, t -> log.error("State handler terminated with an error", t),
                            () -> log.debug("State handler completed")));

            sink.onCancel(composite);
        });
    }

    private Mono<Authorization> redirectToDc(RpcException rpcExc,
                                             MTProtoClient tmpClient, MTProtoClientGroup clientGroup,
                                             StoreLayout storeLayout, DcOptions dcOptions) {
        return Mono.defer(() -> {
            String msg = rpcExc.getError().errorMessage();
            if (!msg.startsWith("USER_MIGRATE_"))
                return Mono.error(new IllegalStateException("Unexpected type of DC redirection", rpcExc));

            int dcId = Integer.parseInt(msg.substring(13));
            log.info("Redirecting to the DC {}", dcId);

            return Mono.justOrEmpty(dcOptions.find(DataCenter.Type.REGULAR, dcId))
                    // We used default DcOptions which may be outdated.
                    // Well, let's request dc config and store it
                    .switchIfEmpty(tmpClient.sendAwait(GetConfig.instance())
                            .flatMap(cfg -> storeLayout.onUpdateConfig(cfg)
                                    .then(storeLayout.getDcOptions()))
                            .flatMap(newOpts -> Mono.justOrEmpty(newOpts.find(DataCenter.Type.REGULAR, dcId))
                                    .switchIfEmpty(Mono.error(() -> new IllegalStateException(
                                            "Could not find DC " + dcId + " for redirecting main client in received options: " + newOpts)))))
                    .flatMap(clientGroup::setMain)
                    .flatMap(client -> client.sendAwait(ImmutableImportBotAuthorization.of(0,
                            authResources.getApiId(), authResources.getApiHash(),
                            authResources.getBotAuthToken().orElseThrow())));
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

    private TcpClientResources initTcpClientResources() {
        if (tcpClientResources != null) {
            return tcpClientResources;
        }
        return TcpClientResources.create();
    }

    private EventDispatcher initEventDispatcher() {
        if (eventDispatcher != null) {
            return eventDispatcher;
        }
        return new DefaultEventDispatcher(ForkJoinPoolScheduler.create("t4j-events", 4),
                Sinks.many().multicast().onBackpressureBuffer(Queues.SMALL_BUFFER_SIZE, false),
                Sinks.EmitFailureHandler.FAIL_FAST);
    }

    private UpdateDispatcher initUpdateDispatcher() {
        if (updateDispatcher != null) {
            return updateDispatcher;
        }
        return new SinksUpdateDispatcher();
    }

    private StoreLayout initStoreLayout() {
        if (storeLayout != null) {
            return storeLayout;
        }
        return new FileStoreLayout(new StoreLayoutImpl(c -> c.maximumSize(1000)));
    }

    private DataCenter initDataCenter(DcOptions opts) {
        if (dataCenter != null) {
            return dataCenter;
        }
        return opts.find(DataCenter.Type.REGULAR, 2)
                .orElseThrow(() -> new IllegalStateException("Could not find DC 2 for main client in options: " + opts));
    }

    private DcOptions initDcOptions() {
        if (dcOptions != null) {
            return dcOptions;
        }
        return DcOptions.createDefault(false);
    }

    private RetryBackoffSpec initConnectionRetry() {
        if (connectionRetry != null) {
            return connectionRetry;
        }
        return Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(5));
    }

    private RetryBackoffSpec initAuthRetry() {
        if (authRetry != null) {
            return authRetry;
        }
        return Retry.fixedDelay(5, Duration.ofSeconds(3));
    }
}
