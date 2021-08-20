package telegram4j.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

import java.util.Optional;

@Value.Immutable
@JsonSerialize(as = ImmutableVideoData.class)
@JsonDeserialize(as = ImmutableVideoData.class)
public interface VideoData {

    static ImmutableVideoData.Builder builder() {
        return ImmutableVideoData.builder();
    }

    @JsonProperty("file_id")
    String fileId();

    @JsonProperty("file_unique_id")
    String fileUniqueId();

    int width();

    int height();

    // in seconds
    int duration();

    Optional<PhotoSizeData> thumb();

    @JsonProperty("file_name")
    Optional<String> fileName();

    @JsonProperty("mime_type")
    Optional<String> mimeType();

    @JsonProperty("file_size")
    Optional<Integer> fileSize();
}
