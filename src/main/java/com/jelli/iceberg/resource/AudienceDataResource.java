package com.jelli.iceberg.resource;


import com.jelli.dataservicereader.api.AudienceStatsApi;
import com.jelli.dataservicereader.components.audience.domain.AudienceStatsResponse;
import com.jelli.iceberg.service.dsr.IcebergSparkDsrService;
import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/audience")
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class AudienceDataResource {

    private final IcebergSparkDsrService audienceDataService;

    @PostMapping("/")
    public ResponseEntity<AudienceStatsResponse> processAudienceDataRequest(
            @RequestBody AudienceStatsApi.AudienceStatsRequest audienceStatsRequest) throws NoSuchTableException {
        //AudienceStatsResponse result = audienceDataService.getAudienceStats(audienceStatsRequest);
        return ResponseEntity.ok(null);
    }
}
