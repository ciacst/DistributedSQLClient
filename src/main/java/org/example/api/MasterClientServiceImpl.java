package org.example.api;
import org.example.util.TableRouter;

public class MasterClientServiceImpl implements MasterClientService {

    private TableRouter router;
    @Override
    public String GetRegionServer(String SQL) {
        return router.getServersForSql(SQL);
    }

    public MasterClientServiceImpl() {
        router = new TableRouter();
    }
}
