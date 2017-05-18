package com.mkwhitacre.conway.spark.streaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.ObjectInputStreamWithLoader;
import scala.Function1;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;


public class SerializeFn implements MapFunction<byte[], SparkCell> {
    @Override
    public SparkCell call(byte[] value) throws Exception {

        //java.lang.ClassCastException: [B cannot be cast to com.mkwhitacre.conway.spark.streaming.SparkCell(..)
        ByteArrayInputStream is = new ByteArrayInputStream(value);
        ObjectInputStream inStream = new ObjectInputStream(is);

        return (SparkCell) inStream.readObject();
    }
}
