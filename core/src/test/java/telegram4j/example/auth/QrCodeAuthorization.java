package telegram4j.example.auth;

import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.Logger;
import reactor.util.Loggers;
import reactor.util.retry.Retry;
import telegram4j.core.AuthorizationResources;
import telegram4j.mtproto.DataCenter;
import telegram4j.mtproto.DcId;
import telegram4j.mtproto.RpcException;
import telegram4j.mtproto.client.MTProtoClientGroup;
import telegram4j.mtproto.store.StoreLayout;
import telegram4j.mtproto.util.CryptoUtil;
import telegram4j.mtproto.util.ResettableInterval;
import telegram4j.tl.BaseUser;
import telegram4j.tl.UpdateLoginToken;
import telegram4j.tl.UpdateShort;
import telegram4j.tl.auth.BaseAuthorization;
import telegram4j.tl.auth.BaseLoginToken;
import telegram4j.tl.auth.LoginTokenMigrateTo;
import telegram4j.tl.auth.LoginTokenSuccess;
import telegram4j.tl.request.auth.ImmutableExportLoginToken;
import telegram4j.tl.request.auth.ImmutableImportLoginToken;

import java.io.InputStreamReader;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Scanner;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * QR-code user authorization implementation that displays a qrcode of unicode chars to stdout.
 * <p>
 * This implementation can work only on linux with installed <b>qrencode</b> lib.
 */
public class QrCodeAuthorization {

    private static final Logger log = Loggers.getLogger(QrCodeAuthorization.class);

    public static Mono<BaseAuthorization> authorize(MTProtoClientGroup clientGroup, StoreLayout storeLayout,
                                                    AuthorizationResources authResources) {
        return Mono.create(sink -> {
            AtomicBoolean complete = new AtomicBoolean();

            int apiId = authResources.getApiId();
            String apiHash = authResources.getApiHash();
            ResettableInterval inter = new ResettableInterval(Schedulers.boundedElastic());

            var listenTokens = clientGroup.updates().on(UpdateShort.class)
                    .takeUntil(u -> complete.get())
                    .filter(u -> u.update().identifier() == UpdateLoginToken.ID)
                    .flatMap(l -> clientGroup.send(DcId.main(), ImmutableExportLoginToken.of(apiId, apiHash, List.of())))
                    .flatMap(token -> switch (token.identifier()) {
                        case LoginTokenSuccess.ID -> {
                            var success = (LoginTokenSuccess) token;
                            yield Mono.just(success.authorization());
                        }
                        case LoginTokenMigrateTo.ID -> {
                            var migrate = (LoginTokenMigrateTo) token;
                            log.info("Redirecting to the DC {}", migrate.dcId());
                            yield storeLayout.getDcOptions()
                                    // TODO: request help.GetConfig if DC not found
                                    .map(dcOpts -> dcOpts.find(DataCenter.Type.REGULAR, migrate.dcId())
                                            .orElseThrow(() -> new IllegalStateException("Could not find DC " + migrate.dcId()
                                                    + " for redirecting main client")))
                                    .flatMap(clientGroup::setMain)
                                    .flatMap(client -> client.sendAwait(ImmutableImportLoginToken.of(migrate.token())))
                                    .cast(LoginTokenSuccess.class);
                        }
                        default -> Flux.error(new IllegalStateException("Unexpected type of LoginToken: " + token));
                    })
                    .onErrorResume(RpcException.isErrorMessage("SESSION_PASSWORD_NEEDED"), e -> {
                        inter.dispose();
                        TwoFactorAuthHandler tfa = new TwoFactorAuthHandler(clientGroup, sink);
                        return tfa.begin2FA()
                                .retryWhen(Retry.indefinitely()
                                        .filter(RpcException.isErrorMessage("PASSWORD_HASH_INVALID")));
                    })
                    .cast(BaseAuthorization.class)
                    .doOnNext(l -> {
                        BaseUser b = (BaseUser) l.user();
                        StringJoiner j = new StringJoiner(" ");
                        Optional.ofNullable(b.firstName()).ifPresent(j::add);
                        Optional.ofNullable(b.lastName()).ifPresent(j::add);
                        String name = (name = j.toString()).isEmpty() ? "unknown" : name;
                        log.info("Successfully login as {}", name);
                        complete.set(true);
                        inter.dispose();
                        sink.success(l);
                    })
                    .subscribe();

            inter.start(Duration.ofMinutes(1)); // stub period

            var qrDisplay = inter.ticks()
                    .flatMap(tick -> clientGroup.send(DcId.main(), ImmutableExportLoginToken.of(apiId, apiHash, List.of())))
                    .cast(BaseLoginToken.class)
                    .doOnNext(b -> {
                        Duration dur = Duration.ofSeconds(b.expires() - (System.currentTimeMillis() / 1000));
                        String token = java.util.Base64.getUrlEncoder().encodeToString(CryptoUtil.toByteArray(b.token()));

                        String url = "tg://login?token=" + token;
                        synchronized (System.out) {
                            System.out.println("QR code (you have " + dur.getSeconds() + " seconds):");
                            System.out.println(generateQr(url));
                            System.out.println();
                        }

                        inter.start(dur, dur);
                    })
                    .subscribe();

            sink.onCancel(Disposables.composite(listenTokens, qrDisplay));
        });
    }

    private static String generateQr(String text) {
        try {
            Process v = new ProcessBuilder("qrencode", "-t", "UTF8", text)
                    .redirectOutput(ProcessBuilder.Redirect.INHERIT)
                    .start();

            Scanner stre = new Scanner(new InputStreamReader(v.getInputStream()));

            StringBuilder b = new StringBuilder();
            while (stre.hasNext()) {
                b.append(stre.nextLine()).append('\n');
            }
            return b.toString();
        } catch (Throwable t) {
            throw Exceptions.propagate(t);
        }
    }
}
