package dev.ikm.load;

import dev.ikm.snomedct.entitytransformer.SnomedUtility;
import dev.ikm.tinkar.common.id.IntIds;
import dev.ikm.tinkar.common.id.PublicId;
import dev.ikm.tinkar.common.id.PublicIds;
import dev.ikm.tinkar.common.service.PrimitiveData;
import dev.ikm.tinkar.common.service.ServiceKeys;
import dev.ikm.tinkar.common.service.ServiceProperties;
import dev.ikm.tinkar.common.util.time.DateTimeUtil;
import dev.ikm.tinkar.common.util.uuid.UuidT5Generator;
import dev.ikm.tinkar.common.util.uuid.UuidUtil;
import dev.ikm.tinkar.coordinate.Calculators;
import dev.ikm.tinkar.coordinate.view.calculator.ViewCalculatorWithCache;
import dev.ikm.tinkar.entity.*;
import dev.ikm.tinkar.entity.graph.adaptor.axiom.LogicalExpression;
import dev.ikm.tinkar.entity.transaction.Transaction;
import dev.ikm.tinkar.ext.lang.owl.SctOwlUtilities;
import dev.ikm.tinkar.terms.EntityProxy;
import dev.ikm.tinkar.terms.State;
import dev.ikm.tinkar.terms.TinkarTerm;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.primitive.IntLists;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Hello world!
 *
 */
public class ImportAndTransform
{
    private static final Logger LOG = LoggerFactory.getLogger(ImportAndTransform.class);
    public static final EntityProxy.Pattern COMMENT_PATTERN =
            EntityProxy.Pattern.make("Comment pattern", UUID.fromString("3734fb0a-4c14-5831-9a61-4743af609e7a"));

    enum Fields {
        ID, EFFECTIVE_TIME, ACTIVE, MODULE_ID, REFSET_ID, REFERENCED_COMPONENT_ID, OWL_EXPRESSION;
    }
    public static void main( String[] args )
    {

        // Open database

        ServiceProperties.set(ServiceKeys.DATA_STORE_ROOT, new File("target/database/tinkar-snomedct-international-20241001"));
        LOG.info("JVM Version: " + System.getProperty("java.version"));
        LOG.info("JVM Name: " + System.getProperty("java.vm.name"));
        LOG.info(ServiceProperties.jvmUuid());
        PrimitiveData.selectControllerByName("Open SpinedArrayStore");
        PrimitiveData.start();
        Transaction transaction = Transaction.make();


        // File is brought in during the verify phase, so is available during project run.
        try (ZipFile zipFile = new ZipFile(new File("target/terminology/snomed-ct-us-1000124_20240901T120000Z.zip"), Charset.forName("UTF-8"))) {
            ZipEntry entry = zipFile.getEntry("SnomedCT_InternationalRF2_PRODUCTION_20241001T120000Z/Full/Terminology/sct2_sRefset_OWLExpressionFull_INT_20241001.txt");
            LOG.info( "Found entry: {}", entry.getName() );
            try (InputStream inputStream = zipFile.getInputStream(entry)) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, Charset.forName("UTF-8")))) {
                    boolean firstLine = true;
                    int recordCount = 0;
                    for (String line = br.readLine(); line != null; line = br.readLine()) {
                        String[] fields = line.split( "\t" );
                        if (firstLine) {
                            for (Fields field : Fields.values()) {
                                LOG.info( field.name() + ": " + fields[field.ordinal()] );
                            }
                            Optional<Entity<EntityVersion>> commentPattern = Entity.get(COMMENT_PATTERN);
                            LOG.info( "Comment pattern: " + commentPattern );
                            firstLine = false;
                        } else {
                            // import into database.
                            /*
id	effectiveTime	active	moduleId	refsetId	referencedComponentId	owlExpression
80001735-381a-4c86-a986-a6ebd875f6c7	20190731	1	900000000000207008	733073007	42061009	SubClassOf(:42061009 :398334008)
80002779-6efa-491f-88d3-8a393898bbe4	20190731	1	900000000000207008	733073007	239604004	SubClassOf(:239604004 ObjectIntersectionOf(:265114005 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:260686004 :129377008) ObjectSomeValuesFrom(:405813007 :76505004)))))
                             */

                            State state = switch(fields[Fields.ACTIVE.ordinal()]) {
                                case "1" -> State.ACTIVE;
                                case "0" -> State.INACTIVE;
                                default -> throw new RuntimeException("Unknown active value: " + fields[Fields.ACTIVE.ordinal()]);
                            };
                            long effectiveTimeInEpochMs = DateTimeUtil.compressedParse(fields[Fields.EFFECTIVE_TIME.ordinal()] + "T000000Z");
                            PublicId authorId = TinkarTerm.USER.publicId();
                            PublicId moduleId = PublicIds.of(UuidUtil.fromSNOMED(fields[Fields.MODULE_ID.ordinal()]));
                            PublicId pathId = TinkarTerm.DEVELOPMENT_PATH.publicId();

                            //State state, long time, PublicId authorId, PublicId moduleId, PublicId pathId
                            StampEntity stampForSemantic = transaction.getStamp(state, effectiveTimeInEpochMs, authorId, moduleId, pathId);

                            SemanticRecord semanticRecord = SemanticRecord.build(UUID.fromString(fields[Fields.ID.ordinal()]), // Semantic UUID
                                    COMMENT_PATTERN.nid(), // Pattern nid
                                    PrimitiveData.nid(UuidUtil.fromSNOMED(fields[Fields.REFERENCED_COMPONENT_ID.ordinal()])),
                                    stampForSemantic.lastVersion(),
                                    Lists.immutable.of(fields[Fields.OWL_EXPRESSION.ordinal()]));
                            Entity.provider().putEntity(semanticRecord);
                            if (recordCount < 25) {
                                LOG.info( "Created semantic record: {}", semanticRecord );
                            }
                            recordCount++;
                        }
                    }
                    LOG.info( "Imported {} OWL semantic records. ", recordCount );
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        findRecordsForMeglumineAntimoniateOnlyProduct();
        countOwlRecordsForConcepts();
        processLymeDiseaseOfInnerEar(transaction);
        processMeglumineAntimoniate(transaction);
        processActiveIngredient(transaction);
        transaction.commit();
        PrimitiveData.save();
    }

    private static void findRecordsForMeglumineAntimoniateOnlyProduct() {
        final EntityProxy.Concept MEGLUMINE_ANTIMONIATE =
                EntityProxy.Concept.make("Meglumine antimoniate only product in parenteral dose form ", UUID.fromString("8cd1a08b-94ce-3c14-8fe6-d5e82983eff9"));

        Entity.provider().forEachSemanticForComponentOfPattern(MEGLUMINE_ANTIMONIATE.nid(), COMMENT_PATTERN.nid(), semanticRecord -> {
            LOG.info( "Semantic record for:\n {}", semanticRecord);
        });
    }

    private static void countOwlRecordsForConcepts() {
        LongAdder conceptCount = new LongAdder();
        LongAdder conceptsWithNoOwlStrings = new LongAdder();
        LongAdder conceptsWithOneOwlStrings = new LongAdder();
        LongAdder conceptsWithTwoOwlStrings = new LongAdder();
        LongAdder conceptsWithThreeOwlStrings = new LongAdder();
        LongAdder conceptsWithFourOwlStrings = new LongAdder();
        LongAdder conceptsWithFiveOwlStrings = new LongAdder();
        LongAdder conceptsWithSixOrMoreOwlStrings = new LongAdder();
        MutableIntList conceptsWithSixOrMore = IntLists.mutable.empty();

        PrimitiveData.get().forEachConceptNid(conceptNid -> {
            conceptCount.increment();
            AtomicInteger semanticCount = new AtomicInteger();
            Entity.provider().forEachSemanticForComponentOfPattern(conceptNid,COMMENT_PATTERN.nid(), semanticRecord -> {
                semanticCount.incrementAndGet();
            });
            switch (semanticCount.get()) {
                case 0 -> conceptsWithNoOwlStrings.increment();
                case 1 -> conceptsWithOneOwlStrings.increment();
                case 2 -> conceptsWithTwoOwlStrings.increment();
                case 3 -> conceptsWithThreeOwlStrings.increment();
                case 4 -> conceptsWithFourOwlStrings.increment();
                case 5 -> conceptsWithFiveOwlStrings.increment();
                default -> {
                    conceptsWithSixOrMoreOwlStrings.increment();
                    conceptsWithSixOrMore.add(conceptNid);
                }
            }
        });
        ;
        LOG.info( "Total concept count: {}", conceptCount.longValue() );
        LOG.info( "Total concept count with no owl strings: {}", conceptsWithNoOwlStrings.longValue() );
        LOG.info( "Total concept count with one owl strings: {}", conceptsWithOneOwlStrings.longValue() );
        LOG.info( "Total concept count with two owl strings: {}", conceptsWithTwoOwlStrings.longValue() );
        LOG.info( "Total concept count with three owl strings: {}", conceptsWithThreeOwlStrings.longValue() );
        LOG.info( "Total concept count with four owl strings: {}", conceptsWithFourOwlStrings.longValue() );
        LOG.info( "Total concept count with five owl strings: {}", conceptsWithFiveOwlStrings.longValue() );
        LOG.info( "Total concept count with six or more owl strings: {}", conceptsWithSixOrMoreOwlStrings.longValue() );
        LOG.info( "Concepts with with six or more owl strings: {}", IntIds.list.of(conceptsWithSixOrMore.toArray()) );

        conceptsWithSixOrMore.forEach(conceptNid -> {
            StringBuilder sb = new StringBuilder("\n-------------------------------------------");
            sb.append( "\n\nSemantic records for concept ");
            sb.append(PrimitiveData.text(conceptNid));
            sb.append( " with > 6 owl strings:\n");
            Entity.provider().forEachSemanticForComponentOfPattern(conceptNid,COMMENT_PATTERN.nid(),
                    semanticRecord -> {
                        sb.append( "\n\n");
                        sb.append(semanticRecord);

                    });
            sb.append("\n-------------------------------------------");
            LOG.info( sb.toString() );
        });

    }

    private static void processLymeDiseaseOfInnerEar(Transaction transaction) {
        final EntityProxy.Concept LYME_DISEASE_OF_INNER_EAR =
                EntityProxy.Concept.make("Lyme disease of inner ear", UUID.fromString("7df3b9f1-dcf7-3aa2-886b-7b80f583bb10"));
        LogicalExpression expression = extractLogicalExpression(LYME_DISEASE_OF_INNER_EAR);
        writeStatedLogicalExpression(transaction, LYME_DISEASE_OF_INNER_EAR, expression);
        LOG.info("\n" + PrimitiveData.text(LYME_DISEASE_OF_INNER_EAR.nid()) + "\n" + expression.toString() );
    }

    private static void processMeglumineAntimoniate(Transaction transaction) {
        final EntityProxy.Concept MEGLUMINE_ANTIMONIATE =
                EntityProxy.Concept.make("Meglumine antimoniate only product in parenteral dose form ", UUID.fromString("8cd1a08b-94ce-3c14-8fe6-d5e82983eff9"));
        LogicalExpression expression = extractLogicalExpression(MEGLUMINE_ANTIMONIATE);

        writeStatedLogicalExpression(transaction, MEGLUMINE_ANTIMONIATE, expression);

        LOG.info("\n" + PrimitiveData.text(MEGLUMINE_ANTIMONIATE.nid()) + "\n" + expression.toString() );
    }

    private static void processActiveIngredient(Transaction transaction) {

        EntityProxy.Concept HAS_ACTIVE_INGREDIENT =
                EntityProxy.Concept.make("Has Active Ingredient", UUID.fromString("65bf3b7f-c854-36b5-81c3-4915461020a8"));
        LogicalExpression expression = extractLogicalExpression(HAS_ACTIVE_INGREDIENT);

        writeStatedLogicalExpression(transaction, HAS_ACTIVE_INGREDIENT, expression);

        LOG.info("\n" + PrimitiveData.text(HAS_ACTIVE_INGREDIENT.nid()) + "\n" + expression.toString() );
    }

    private static void writeStatedLogicalExpression(Transaction transaction, EntityProxy.Concept conceptProxy, LogicalExpression expression) {
        long effectiveTimeInEpochMs = System.currentTimeMillis();
        PublicId authorId = TinkarTerm.USER.publicId();
        PublicId moduleId = TinkarTerm.SOLOR_MODULE.publicId();
        PublicId pathId = TinkarTerm.DEVELOPMENT_PATH.publicId();

        //State state, long time, PublicId authorId, PublicId moduleId, PublicId pathId
        StampEntity stampForSemantic = transaction.getStamp(State.ACTIVE, effectiveTimeInEpochMs, authorId, moduleId, pathId);

        UUID semanticUuid = UuidT5Generator.singleSemanticUuid(TinkarTerm.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.publicId(),
                conceptProxy.publicId());

        SemanticRecord semanticRecord = SemanticRecord.build(semanticUuid, // Semantic UUID
                TinkarTerm.EL_PLUS_PLUS_STATED_AXIOMS_PATTERN.nid(), // Pattern nid
                conceptProxy.nid(),
                stampForSemantic.lastVersion(),
                Lists.immutable.of(expression.sourceGraph()));
        Entity.provider().putEntity(semanticRecord);
    }

    private static LogicalExpression extractLogicalExpression(EntityProxy.Concept concept) {
        ViewCalculatorWithCache viewCalculator = Calculators.View.Default();
        StringBuilder propertyBuilder = new StringBuilder();
        StringBuilder classBuilder = new StringBuilder();

        Entity.provider().forEachSemanticForComponentOfPattern(concept.nid(),COMMENT_PATTERN.nid(),
                semanticRecord -> {
                    viewCalculator.stampCalculator().latest(semanticRecord).ifPresent(semanticVersion -> {
                        if (semanticVersion.active()) {
                            String owlExpressionSctIds = semanticVersion.fieldValues().get(0).toString();
                            String owlExpressionPublicIds = SnomedUtility.owlAxiomIdsToPublicIds(owlExpressionSctIds);
                            if (owlExpressionPublicIds.toLowerCase().contains("property")) {
                                propertyBuilder.append(" ").append(owlExpressionPublicIds);
                                if (!owlExpressionPublicIds.toLowerCase().contains("objectpropertychain")) {
                                    //TODO ask Michael Lawley if this is ok...
                                    String tempExpression = owlExpressionPublicIds.toLowerCase().replace("subobjectpropertyof", " subclassof");
                                    tempExpression = tempExpression.toLowerCase().replace("subdatapropertyof", " subclassof");
                                    classBuilder.append(" ").append(tempExpression);
                                }
                            } else {
                                classBuilder.append(" ").append(owlExpressionPublicIds);
                            }
                        }
                    });
                });


        String owlClassExpressionsToProcess = classBuilder.toString();
        String owlPropertyExpressionsToProcess = propertyBuilder.toString();

        try {
            return SctOwlUtilities.sctToLogicalExpression(
                    owlClassExpressionsToProcess,
                    owlPropertyExpressionsToProcess);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
