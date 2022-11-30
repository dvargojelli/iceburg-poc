package com.jelli.iceberg.service.dsr;

import com.jelli.dataservicereader.api.AudienceStatsApi;
import com.jelli.dataservicereader.components.audience.AudienceStats;
import com.jelli.dataservicereader.components.audience.domain.AudienceStatRequestTerm;
import com.jelli.dataservicereader.components.audience.domain.AudienceStatsResponse;


import com.jelli.dataservicereader.components.audience.model.PlanDao;
import com.jelli.dataservicereader.components.audience.stats.StatsComputer;
import com.jelli.dataservicereader.components.databook.AudienceDataRow;
import com.jelli.dataservicereader.components.databook.DatabookCompoundService;
import com.jelli.dataservicereader.components.databook.DatabookIdentifier;
import com.jelli.dataservicereader.components.databook.DatabookRequest;
import com.jelli.dataservicereader.components.databook.PopulationData;
import com.jelli.iceberg.model.dao.SimpleAudienceDataDao;
import com.jelli.iceberg.model.tables.impl.SimpleAudienceDataTable;
import com.jelli.iceberg.service.delegates.IcebergQuery;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;

import static org.apache.spark.sql.functions.sum;


@Log4j
@Component
public class IcebergSparkDsrService implements AudienceStats {

    private final SimpleAudienceDataDao simpleAudienceDataDao;
    private final DatabookCompoundService databookCompoundService;
    private final PlanDao planDao;

    @Autowired
    public IcebergSparkDsrService(SimpleAudienceDataDao simpleAudienceDataDao, PlanDao planDao, DatabookCompoundService databookCompoundService) throws NoSuchTableException {
        this.simpleAudienceDataDao = simpleAudienceDataDao;
        this.planDao = planDao;
        this.databookCompoundService = databookCompoundService;
    }


    @Override
    public AudienceStatsResponse getAudienceStats(int planId) {
        final DatabookIdentifier databookIdentifier = planDao.getDatabook( planId );
        final Collection<AudienceStatRequestTerm> requestTerms = planDao.getAudienceStatRequestTerms( planId );

        return getAudienceStats( databookIdentifier,
                planDao.getDemographics( planId ),
                requestTerms,
                DatabookRequest.toInternalOptions( planDao.getRequestOptions( planId ) ) );
    }

    @Override
    public AudienceStatsResponse getAudienceStats(DatabookIdentifier databookIdentifier,
                                                  Collection<com.jelli.commons.ratings.Demographic> demographics,
                                                  Collection<AudienceStatRequestTerm> requestTerms,
                                                  DatabookRequest.DatabookRequestOptions requestOptions) {
        final DatabookRequest databookRequest = new DatabookRequest( databookIdentifier, demographics, requestTerms,
                requestOptions );
        final PopulationData populationData = databookCompoundService.getPopulationData( databookRequest );
        final Collection<AudienceDataRow> audienceData = getAudienceDataRows( requestTerms, databookRequest );
        return new StatsComputer( requestTerms, audienceData, populationData, true ).calculateStats();
    }


    private Collection<AudienceDataRow> getAudienceDataRows(
            Collection<AudienceStatRequestTerm> requestTerms,
            DatabookRequest databookRequest) {
        return simpleAudienceDataDao.getCumeFor(requestTerms, databookRequest);
    }

}
