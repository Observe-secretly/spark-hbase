package cn.tsign.spark.decoder;

import com.alibaba.fastjson.JSON;

import cn.tsign.entity.TrackEntity;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class TrackDecoder implements Decoder<TrackEntity> {

    VerifiableProperties prop;

    public TrackDecoder(VerifiableProperties prop){
        this.prop = prop;
    }

    @Override
    public TrackEntity fromBytes(byte[] arg0) {
        try {
            return JSON.parseObject(new String(arg0), TrackEntity.class);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("序列化失败-->" + new String(arg0));
        }

        return null;
    }

}
