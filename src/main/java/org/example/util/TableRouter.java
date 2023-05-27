package org.example.util;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.example.util.MapSerializer;

import javax.naming.ldap.Control;

enum SqlType {
    SELECT,
    UPDATE,
    INSERT,
    DELETE,
    CREATE,
    DROP,
    INVALID
}
public class TableRouter {
    private ConcurrentHashMap<String, String> TableRegion;
    private ConcurrentHashMap<String, List<String>> Regions;
    private ConcurrentHashMap<String, Set<String>> RegionTables;

    private String RegionPath = "RegionStoreFile";
    private String TablePath = "TableStoreFile";

    MapSerializer<String, List<String>> RegionSerializer;

    MapSerializer<String, Set<String>> TableSerializer;

    private void WriteRegion() {
        try {
            RegionSerializer.serializeMapToFile(Regions, RegionPath);
        } catch (IOException e) {
            System.out.println("Fail to write region, info:");
            System.out.println(e.toString());
        }
    }

    private void WriteTable() {
        try {
            TableSerializer.serializeMapToFile(RegionTables, TablePath);
        } catch (IOException e) {
            System.out.println("Fail to write region, info:");
            System.out.println(e.toString());
        }
    }

    private void init() {
        System.out.print("initing...");
        try {
            Regions = RegionSerializer.deserializeMapFromFile(RegionPath);
            RegionTables = TableSerializer.deserializeMapFromFile(TablePath);
        } catch (Exception e) {
            System.out.println("Fail to read, info:");
            System.out.println(e.toString());
            Regions = new ConcurrentHashMap<>();
            RegionTables = new ConcurrentHashMap<>();
        }

        TableRegion = new ConcurrentHashMap<>();
        for (Map.Entry<String, Set<String>> entry1 : RegionTables.entrySet()) {
            String key = entry1.getKey();
            Set<String> value = entry1.getValue();
            for (String valueElement : value) {
//                System.out.println("Key: " + key + ", Value: " + valueElement);
                TableRegion.put(valueElement, key);
            }
        }
        System.out.print("Done!");
    }

    public SqlType getTypeOfSql(String sql) {
        String[] tokens = sql.split(" ");
        SqlType type = SqlType.INVALID;
        switch (tokens[0].toLowerCase()) {
            case "create": type = SqlType.CREATE; break;
            case "insert": type = SqlType.INSERT; break;
            case "update": type = SqlType.UPDATE; break;
            case "delete": type = SqlType.DELETE; break;
            case "select": type = SqlType.SELECT; break;
            case "drop":   type = SqlType.DROP;   break;
        }
        return type;
    }

    public String getTableOfQuerySql(String sql, SqlType type) {
        String[] tokens = sql.split(" ");
        if(type.equals(SqlType.SELECT) || type.equals(SqlType.DELETE)) {
            for(int i = 0; i < tokens.length; i++) {
                if(tokens[i].toLowerCase().equals("from") && i + 1 < tokens.length)
                    return tokens[i+1];
            }
        }
        else if(type.equals(SqlType.INSERT)) {
            for(int i = 0; i < tokens.length; i++) {
                if(tokens[i].toLowerCase().equals("into") && i + 1 < tokens.length)
                    return tokens[i+1];
            }
        }
        else if(type.equals(SqlType.UPDATE))
            return tokens[1];
        return "";
    }

    public String getTableOfControlSql(String sql) {
        String[] tokens = sql.split(" ");
        for(int i = 0; i < tokens.length; i++) {
            if(tokens[i].toLowerCase().equals("table") && i + 1 < tokens.length)
                return tokens[i+1];
        }
        return "";
    }

    public void addRegionServer(String regionId, String server) {
        if(!Regions.containsKey(regionId)) {
            System.out.println("new region " + regionId);
            System.out.println("servers:");
            String[] ServerIdAndIps = server.split(",");
            List<String> serverIps = new ArrayList<>();
            for(int i = 0; i < ServerIdAndIps.length; i++) {
                String[] ids = ServerIdAndIps[i].split(":");
                String ipPort = ids[1]+":"+ids[2];
                serverIps.add(ipPort);
                System.out.println(ipPort);
            }
            RegionTables.put(regionId, new HashSet<>());
            Regions.put(regionId, serverIps);
            WriteTable();
            WriteRegion();
        }
    }

    public void deleteRegionServer(String regionId){
        if(Regions.containsKey(regionId)) {
            System.out.println("delete region " + regionId);

            RegionTables.remove(regionId);
            Regions.remove(regionId);
            WriteTable();
            WriteRegion();
        }
    }

    public List<String> getServersOfTable(String table) {
        if(!TableRegion.containsKey(table))
            return null;
        String RegionId = TableRegion.get(table);
        return Regions.get(RegionId);
    }

    public String GetMinRegion(){
        String regionId = null;
        Integer minSize = Integer.MAX_VALUE;
        for(Map.Entry<String, Set<String>> entry : RegionTables.entrySet()) {
            if(entry.getValue().size() < minSize) {
                minSize = entry.getValue().size();
                regionId = entry.getKey();
            }
        }

        return regionId;
    }


    String ControlTable(String table, SqlType type) {
        if(type.equals(SqlType.DROP)) {
            String region = TableRegion.get(table);
            if(region != null) {
                RegionTables.get(region).remove(table);
                TableRegion.remove(table);
                WriteTable();
                return region;
            }
        } else if (type.equals(SqlType.CREATE)) {
            if(TableRegion.containsKey(table)) {
                return null;
            }
            String regionId = null;
            Integer minSize = Integer.MAX_VALUE;
            for(Map.Entry<String, Set<String>> entry : RegionTables.entrySet()) {
                if(entry.getValue().size() < minSize) {
                    minSize = entry.getValue().size();
                    regionId = entry.getKey();
                }
            }
            if(regionId != null) {
                RegionTables.get(regionId).add(table);
                TableRegion.put(table, regionId);
                WriteTable();
                return regionId;
            }
        }
        return null;
    }

    // return : region id for the table
    public String getRegionAndExecute(String sql) {
        SqlType type = getTypeOfSql(sql);
        String table = "";
        if(type.equals(SqlType.CREATE) || type.equals(SqlType.DROP)) {
            table = getTableOfControlSql(sql);
            if(table.equals(""))
                return "";
            return ControlTable(table, type);
        }
        else if(type.equals(SqlType.INVALID)) {
            return "";
        }
        else {
            table = getTableOfQuerySql(sql, type);
            if (table.equals(""))
                return "";
            return TableRegion.get(table);
        }
    }

    public String getRegionPeers(String region) {
        List<String> Servers = Regions.get(region);
        if(Servers == null)
            return "";
        String res = "";
        for (int i = 0; i < Servers.size(); i++) {
            if (i > 0) {
                res += ",";
            }
            res += Integer.toString(i);
            res += ":";
            res += Servers.get(i);
        }
        return res;
    }

    public ConcurrentHashMap<String, String> getTableRegion() {
        return TableRegion;
    }

    public ConcurrentHashMap<String, List<String>> getRegions() {
        return Regions;
    }

    public ConcurrentHashMap<String, Set<String>> getRegionTables() {
        return RegionTables;
    }

    public TableRouter() {
        TableRegion = new ConcurrentHashMap<>();
        Regions = new ConcurrentHashMap<>();
        RegionTables = new ConcurrentHashMap<>();

//        // todo : use files for initialization and persistence.
        List<String> region1 = new ArrayList<>();
        region1.add("127.0.0.1:8080");
        region1.add("127.0.0.1:8081");
        region1.add("127.0.0.1:8082");

        List<String> region2 = new ArrayList<>();
        region2.add("127.0.0.1:8083");
        region2.add("127.0.0.1:8084");
        region2.add("127.0.0.1:8085");

        Regions.put("region1", region1);
        Regions.put("region2", region2);
        RegionTables.put("region1", new HashSet<>());
        RegionTables.put("region2", new HashSet<>());

        RegionSerializer = new MapSerializer<>();
        TableSerializer = new MapSerializer<>();
        WriteTable();
        WriteRegion();
    }

    public TableRouter(String Region, String Table) {
        RegionPath = Region;
        TablePath = Table;
        RegionSerializer = new MapSerializer<>();
        TableSerializer = new MapSerializer<>();
        init();
    }
}

