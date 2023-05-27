package org.example.api;

import java.util.List;

public interface MasterRegionService {
    boolean ReportRegion(String RegionId, String ServerIP);

    boolean ReportFailure(String RegionId, String SQLDump);
}
