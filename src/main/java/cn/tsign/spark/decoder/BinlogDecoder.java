package cn.tsign.spark.decoder;

import com.alibaba.fastjson.JSON;

import cn.tsign.entity.BinlogEntity;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class BinlogDecoder implements Decoder<BinlogEntity> {

    VerifiableProperties prop;

    public BinlogDecoder(VerifiableProperties prop){
        this.prop = prop;
    }

    @Override
    public BinlogEntity fromBytes(byte[] arg0) {
        try {
            String json = new String(arg0);
            BinlogEntity entry = JSON.parseObject(json, BinlogEntity.class);
            return entry;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("序列化失败-->" + new String(arg0));
        }

        return null;
    }

}
