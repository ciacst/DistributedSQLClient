package org.example.api;

import org.example.util.TableRouter;
import java.util.List;

public class MasterRegionServiceImpl implements MasterRegionService {
    TableRouter router;
    public boolean ReportRegion(String RegionId, String ServerIP) {
        router.addRegionServer(RegionId, ServerIP);
        return true;
    }

    public MasterRegionServiceImpl(TableRouter router_) {
        router = router_;
    }
}
