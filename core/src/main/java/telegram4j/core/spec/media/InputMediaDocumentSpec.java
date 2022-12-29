package telegram4j.core.spec.media;

import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;
import telegram4j.core.MTProtoTelegramClient;
import telegram4j.core.util.Variant2;
import telegram4j.mtproto.file.FileReferenceId;
import telegram4j.tl.InputMedia;
import telegram4j.tl.InputMediaDocument;
import telegram4j.tl.InputMediaDocumentExternal;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public final class InputMediaDocumentSpec implements InputMediaSpec {
    @Nullable
    private final FileReferenceId documentFri;
    @Nullable
    private final String documentUrl;
    @Nullable
    private final String query;
    private final Duration autoDeleteDuration;

    private InputMediaDocumentSpec(FileReferenceId documentFri) {
        this.documentFri = Objects.requireNonNull(documentFri);
        this.documentUrl = null;
        this.autoDeleteDuration = null;
        this.query = null;
    }

    private InputMediaDocumentSpec(String documentUrl) {
        this.documentUrl = Objects.requireNonNull(documentUrl);
        this.autoDeleteDuration = null;
        this.documentFri = null;
        this.query = null;
    }

    private InputMediaDocumentSpec(@Nullable FileReferenceId documentFri, @Nullable String documentUrl,
                                   @Nullable String query, @Nullable Duration autoDeleteDuration) {
        this.documentFri = documentFri;
        this.documentUrl = documentUrl;
        this.query = query;
        this.autoDeleteDuration = autoDeleteDuration;
    }

    public Variant2<String, FileReferenceId> document() {
        return Variant2.of(documentUrl, documentFri);
    }

    public Optional<String> query() {
        return Optional.ofNullable(query);
    }

    public Optional<Duration> autoDeleteDuration() {
        return Optional.ofNullable(autoDeleteDuration);
    }

    @Override
    public Mono<InputMedia> asData(MTProtoTelegramClient client) {
        return Mono.fromCallable(() -> {
            Integer ttlSeconds = autoDeleteDuration()
                    .map(Duration::getSeconds)
                    .map(Math::toIntExact)
                    .orElse(null);

            if (documentFri != null) {
                return InputMediaDocument.builder()
                        .id(documentFri.asInputDocument())
                        .query(query)
                        .ttlSeconds(ttlSeconds)
                        .build();
            }
            return InputMediaDocumentExternal.builder()
                    .url(Objects.requireNonNull(documentUrl))
                    .ttlSeconds(ttlSeconds)
                    .build();
        });
    }

    public InputMediaDocumentSpec withDocument(String photoUrl) {
        if (photoUrl.equals(this.documentUrl)) return this;
        return new InputMediaDocumentSpec(null, photoUrl, query, autoDeleteDuration);
    }

    public InputMediaDocumentSpec withDocument(FileReferenceId photoFri) {
        if (photoFri.equals(this.documentFri)) return this;
        return new InputMediaDocumentSpec(photoFri, null, query, autoDeleteDuration);
    }

    public InputMediaDocumentSpec withQuery(@Nullable String value) {
        if (Objects.equals(this.query, value)) return this;
        return new InputMediaDocumentSpec(documentFri, documentUrl, value, autoDeleteDuration);
    }

    public InputMediaDocumentSpec withQuery(Optional<String> optional) {
        return withQuery(optional.orElse(null));
    }

    public InputMediaDocumentSpec withAutoDeleteDuration(@Nullable Duration value) {
        if (Objects.equals(this.autoDeleteDuration, value)) return this;
        return new InputMediaDocumentSpec(documentFri, documentUrl, query, value);
    }

    public InputMediaDocumentSpec withAutoDeleteDuration(Optional<Duration> optional) {
        return withAutoDeleteDuration(optional.orElse(null));
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) return true;
        if (!(o instanceof InputMediaDocumentSpec)) return false;
        InputMediaDocumentSpec that = (InputMediaDocumentSpec) o;
        return Objects.equals(documentFri, that.documentFri) &&
                Objects.equals(documentUrl, that.documentUrl) &&
                Objects.equals(query, that.query) &&
                Objects.equals(autoDeleteDuration, that.autoDeleteDuration);
    }

    @Override
    public int hashCode() {
        int h = 5381;
        h += (h << 5) + Objects.hashCode(documentFri);
        h += (h << 5) + Objects.hashCode(documentUrl);
        h += (h << 5) + Objects.hashCode(query);
        h += (h << 5) + Objects.hashCode(autoDeleteDuration);
        return h;
    }

    @Override
    public String toString() {
        return "InputMediaDocumentSpec{" +
                "document=" + (documentFri != null ? documentFri : documentUrl) +
                ", query='" + query +
                "', autoDeleteDuration=" + autoDeleteDuration +
                '}';
    }

    public static InputMediaDocumentSpec of(FileReferenceId documentFri) {
        return new InputMediaDocumentSpec(documentFri);
    }

    public static InputMediaDocumentSpec of(String documentUrl) {
        return new InputMediaDocumentSpec(documentUrl);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private FileReferenceId documentFri;
        private String documentUrl;
        private String query;
        private Duration autoDeleteDuration;

        private Builder() {
        }

        public Builder from(InputMediaDocumentSpec instance) {
            documentUrl = instance.documentUrl;
            documentFri = instance.documentFri;
            query = instance.query;
            autoDeleteDuration = instance.autoDeleteDuration;
            return this;
        }

        public Builder document(FileReferenceId documentFri) {
            this.documentFri = Objects.requireNonNull(documentFri);
            this.documentUrl = null;
            return this;
        }

        public Builder document(String documentUrl) {
            this.documentUrl = Objects.requireNonNull(documentUrl);
            this.documentFri = null;
            return this;
        }

        public Builder query(@Nullable String query) {
            this.query = query;
            return this;
        }

        public Builder query(Optional<String> query) {
            this.query = query.orElse(null);
            return this;
        }

        public Builder autoDeleteDuration(@Nullable Duration autoDeleteDuration) {
            this.autoDeleteDuration = autoDeleteDuration;
            return this;
        }

        public Builder autoDeleteDuration(Optional<Duration> autoDeleteDuration) {
            this.autoDeleteDuration = autoDeleteDuration.orElse(null);
            return this;
        }

        public InputMediaDocumentSpec build() {
            if (documentFri == null && documentUrl == null) {
                throw new IllegalStateException("Cannot build InputMediaDocumentSpec, 'document' attribute is not set");
            }
            return new InputMediaDocumentSpec(documentFri, documentUrl, query, autoDeleteDuration);
        }
    }
}
