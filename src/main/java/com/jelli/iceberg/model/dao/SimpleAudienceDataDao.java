package com.jelli.iceberg.model.dao;

import com.jelli.dataservicereader.components.audience.domain.AudienceStatRequestTerm;
import com.jelli.dataservicereader.components.databook.AudienceDataRow;
import com.jelli.dataservicereader.components.databook.DatabookRequest;
import com.jelli.iceberg.model.tables.impl.SimpleAudienceDataTable;
import com.jelli.iceberg.service.delegates.IcebergQuery;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;

import static org.apache.spark.sql.functions.sum;

@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Component
@Log4j
public class SimpleAudienceDataDao {

    private final IcebergQuery<Dataset<Row>> icebergQuery;


    public Double getCumeFor(String bookName, String demo, String daypart){
        String query =
                String.format("select * from <TABLE_NAME> where book_name = %sand where daypart = %sand where demo = %s", bookName, daypart, demo);
        Dataset<Row> result = icebergQuery.sql(query, true);
        Double cume = result.agg(sum("cume")).first().getDouble(0);


        log.info("Size is: " + result.count());
        log.info("Cume total is: " + cume);
        return cume;
    }

    public Collection<AudienceDataRow> getCumeFor(Collection<AudienceStatRequestTerm> requestTerms, DatabookRequest databookRequest) {
        return null;
    }
}
