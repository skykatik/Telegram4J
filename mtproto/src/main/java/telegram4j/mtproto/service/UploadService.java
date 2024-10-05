/*
 * Copyright 2023 Telegram4J
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package telegram4j.mtproto.service;

import io.netty.buffer.Unpooled;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.function.TupleUtils;
import reactor.util.Logger;
import reactor.util.Loggers;
import telegram4j.mtproto.DcId;
import telegram4j.mtproto.RpcException;
import telegram4j.mtproto.client.MTProtoClient;
import telegram4j.mtproto.client.MTProtoClientGroup;
import telegram4j.mtproto.file.FileReferenceId;
import telegram4j.mtproto.service.Compatible.Type;
import telegram4j.mtproto.store.StoreLayout;
import telegram4j.tl.FileHash;
import telegram4j.tl.InputFile;
import telegram4j.tl.InputFileLocation;
import telegram4j.tl.InputWebFileLocation;
import telegram4j.tl.request.auth.ImmutableExportAuthorization;
import telegram4j.tl.request.auth.ImmutableImportAuthorization;
import telegram4j.tl.request.help.GetConfig;
import telegram4j.tl.request.upload.ImmutableGetFile;
import telegram4j.tl.request.upload.ImmutableGetFileHashes;
import telegram4j.tl.request.upload.ImmutableGetWebFile;
import telegram4j.tl.storage.FileType;
import telegram4j.tl.upload.BaseFile;
import telegram4j.tl.upload.ImmutableBaseFile;
import telegram4j.tl.upload.ImmutableWebFile;
import telegram4j.tl.upload.WebFile;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class UploadService extends RpcService {
    static final Logger log = Loggers.getLogger(UploadService.class);

    public static final int MIN_PART_SIZE = 1024;
    public static final int MAX_PART_SIZE = 512 * 1024;

    public static final int BIG_FILE_THRESHOLD = 10 * 1024 * 1024;

    static final int MAX_PARTS_COUNT = 4000; // it's for users with tg premium; for other users limit is 3000
    static final long MAX_FILE_SIZE = 4L * 1000 * 1024 * 1024; // 4gb for premium users; for other = 2gb
    static final int MAX_INFLIGHT_REQUESTS = 3; // optimal count of pending upload.getFile requests
    static final String OFFSET_INVALID_MESSAGE = "OFFSET_INVALID";

    public UploadService(MTProtoClientGroup groupManager, StoreLayout storeLayout) {
        super(groupManager, storeLayout);
    }

    public static int suggestParallelism(long size) {
        return size > BIG_FILE_THRESHOLD ? 3 : 1;
    }

    public static int suggestPartSize(long size, int partSize) {
        if (partSize == -1) {
            return MAX_PART_SIZE; // TODO: adaptive part size
        } else if (partSize > MAX_PART_SIZE) {
            throw new IllegalArgumentException(size + " > " + MAX_PART_SIZE);
        } else if (partSize < MIN_PART_SIZE) {
            throw new IllegalArgumentException(size + " < " + MIN_PART_SIZE);
        } else {
            return partSize;
        }
    }

    // additional methods
    // =========================
    // TODO list:
    // upload.getCdnFile#395f69da file_token:bytes offset:long limit:int = upload.CdnFile;
    // upload.reuploadCdnFile#9b2754a8 file_token:bytes request_token:bytes = Vector<FileHash>;
    // upload.getCdnFileHashes#91dc3f31 file_token:bytes offset:long = Vector<FileHash>;

    @Compatible(Type.BOTH)
    public Mono<InputFile> saveFile(UploadOptions options) {
        Objects.requireNonNull(options);

        return new UploadMono(clientGroup, options);
    }

    // upload namespace
    // =========================

    private Flux<BaseFile> getFile0(MTProtoClient client, FileReferenceId fileRefId,
                                    long baseOffset, int limit, boolean precise) {
        AtomicLong offset = new AtomicLong(baseOffset);
        AtomicBoolean complete = new AtomicBoolean();
        ImmutableGetFile request = ImmutableGetFile.of(precise ? ImmutableGetFile.PRECISE_MASK : 0,
                fileRefId.asLocation().orElseThrow(), baseOffset, limit);

        return Flux.range(0, MAX_INFLIGHT_REQUESTS)
                .flatMapSequential(i -> {
                    var requiredDelay = Mono.delay(Duration.ofMillis(20), Schedulers.single());
                    return client.send(request.withOffset(offset.getAndAdd(limit)))
                            .onErrorReturn(UploadService::isOffsetInvalid,
                                    ImmutableBaseFile.builder()
                                            .bytes(Unpooled.EMPTY_BUFFER)
                                            .mtime(0)
                                            .type(FileType.UNKNOWN)
                                            .build())
                            .flatMap(requiredDelay::thenReturn);
                })
                .repeat(() -> !complete.get())
                .cast(BaseFile.class)
                .mapNotNull(part -> {
                    if (part.type() == FileType.UNKNOWN || !part.bytes().isReadable()) { // download completed
                        complete.set(true);
                        return null;
                    }
                    return part;
                });
    }

    private static boolean isOffsetInvalid(Throwable tx) {
        return tx instanceof RpcException && OFFSET_INVALID_MESSAGE.equals(((RpcException) tx).getError().errorMessage());
    }

    @Compatible(Type.BOTH)
    public Flux<BaseFile> getFile(FileReferenceId location,
                                  long offset, int limit, boolean precise) {
        if (offset < 0) return Flux.error(new IllegalArgumentException("offset is negative"));
        if (limit <= 0) return Flux.error(new IllegalArgumentException("limit is not positive"));

        if (!precise) {
            if (offset % (4 * 1024) != 0)
                return Flux.error(new IllegalArgumentException("offset must be divisible by 4KB"));
            if (limit % (4 * 1024) != 0)
                return Flux.error(new IllegalArgumentException("limit must be divisible by 4KB"));
            if ((1024 * 1024) % limit != 0)
                return Flux.error(new IllegalArgumentException("1MB must be divisible by limit"));
        } else {
            if (offset % 1024 != 0)
                return Flux.error(new IllegalArgumentException("offset must be divisible by 1KB"));
            if (limit % 1024 != 0)
                return Flux.error(new IllegalArgumentException("limit must be divisible by 1KB"));
            if (limit > 1024 * 1024)
                return Flux.error(new IllegalArgumentException("limit must not exceed 1MB"));
        }

        if (location.getFileType() == FileReferenceId.Type.WEB_DOCUMENT)
            return Flux.error(new IllegalArgumentException("Web documents can not be downloaded as normal files"));

        DcId dcId = DcId.download(location.getDcId());
        if (location.getDcId() != clientGroup.main().dc().getId()) {
            return sendMain(ImmutableExportAuthorization.of(location.getDcId()))
                    .zipWith(clientGroup.getOrCreateClient(dcId))
                    .flatMap(TupleUtils.function((auth, client) -> client.send(
                                    ImmutableImportAuthorization.of(auth.id(), auth.bytes()))
                            .thenReturn(client)))
                    .flatMapMany(client -> getFile0(client, location, offset, limit, precise));
        }
        return clientGroup.getOrCreateClient(dcId)
                .flatMapMany(client -> getFile0(client, location, offset, limit, precise));
    }

    private Flux<WebFile> getWebFile0(MTProtoClient client, InputWebFileLocation location,
                                      int baseOffset, int limit) {
        return Flux.defer(() -> {
            AtomicInteger offset = new AtomicInteger(baseOffset);
            AtomicBoolean complete = new AtomicBoolean();

            ImmutableGetWebFile request = ImmutableGetWebFile.of(location, baseOffset, limit);

            return Flux.range(0, MAX_INFLIGHT_REQUESTS)
                    .flatMapSequential(i -> client.send(request.withOffset(offset.getAndAdd(limit)))
                            .onErrorReturn(UploadService::isOffsetInvalid,
                                    ImmutableWebFile.builder()
                                            .bytes(Unpooled.EMPTY_BUFFER)
                                            .mtime(0)
                                            .fileType(FileType.UNKNOWN)
                                            .build()))
                    .repeat(() -> !complete.get())
                    .mapNotNull(part -> {
                        offset.addAndGet(limit);
                        if (part.fileType() == FileType.UNKNOWN || !part.bytes().isReadable()) { // download completed
                            complete.set(true);
                            return null;
                        }

                        return part;
                    })
                    .repeat(() -> !complete.get());
        });
    }

    public Flux<WebFile> getWebFile(InputWebFileLocation location, int offset, int limit) {
        return storeLayout.getConfig()
                .switchIfEmpty(sendMain(GetConfig.instance())
                        .flatMap(cfg -> storeLayout.onUpdateConfig(cfg)
                                .thenReturn(cfg)))
                .flatMap(cfg -> {
                    DcId dc = DcId.download(cfg.webfileDcId());
                    return sendMain(ImmutableExportAuthorization.of(cfg.webfileDcId()))
                            .zipWith(clientGroup.getOrCreateClient(dc));
                })
                .flatMap(TupleUtils.function((auth, client) -> client.send(
                                ImmutableImportAuthorization.of(auth.id(), auth.bytes()))
                        .thenReturn(client)))
                .flatMapMany(client -> getWebFile0(client, location, offset, limit));
    }

    @Compatible(Type.BOTH)
    public Mono<List<FileHash>> getFileHashes(InputFileLocation location, long offset) {
        return sendMain(ImmutableGetFileHashes.of(location, offset));
    }
}
