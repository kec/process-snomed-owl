import dev.ikm.tinkar.common.service.ExecutorController;
import dev.ikm.tinkar.common.service.DataServiceController;
import dev.ikm.tinkar.common.service.PrimitiveDataService;

module process.snomed.owl {
    requires dev.ikm.snomedct.entitytransformer;
    requires dev.ikm.tinkar.ext.lang.owl;
    requires dev.ikm.tinkar.coordinate;
    requires dev.ikm.tinkar.entity;
    requires org.eclipse.collections.api;
    requires org.slf4j;

    uses DataServiceController;
    uses ExecutorController;
    uses PrimitiveDataService;
}