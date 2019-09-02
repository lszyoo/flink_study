import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SourceEventSchema implements DeserializationSchema<SourceEvent> {

    // 将数据从 byte[] 转换成 SourceEvent 的反序列化操作
    @Override
    public SourceEvent deserialize(byte[] message) throws IOException {
        SourceEvent sourceEvent = new SourceEvent(new String(message), 13);
        return sourceEvent.sourceEvent;
    }

    @Override
    public boolean isEndOfStream(SourceEvent nextElement) {
        return false;
    }

    // 将数据类型转换成 Flink 系统所支持的数据类型
    @Override
    public TypeInformation<SourceEvent> getProducedType() {
        return TypeInformation.of(SourceEvent.class);
    }
}