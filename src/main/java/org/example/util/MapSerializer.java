package org.example.util;


import java.io.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class MapSerializer<K, V> {

    public void serializeMapToFile(ConcurrentHashMap<K, V> map, String filePath) throws IOException {
        FileOutputStream fos = new FileOutputStream(filePath);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(map);
        oos.close();
    }

    public ConcurrentHashMap<K, V> deserializeMapFromFile(String filePath) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(filePath);
        ObjectInputStream ois = new ObjectInputStream(fis);
        ConcurrentHashMap<K, V> map = (ConcurrentHashMap<K, V>) ois.readObject();
        ois.close();
        return map;
    }
}
