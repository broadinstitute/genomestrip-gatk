package org.broadinstitute.sting.gatk.walkers.annotator;

import org.broad.tribble.vcf.VCFHeaderLineType;
import org.broad.tribble.vcf.VCFInfoHeaderLine;
import org.broadinstitute.sting.gatk.contexts.ReferenceContext;
import org.broadinstitute.sting.gatk.contexts.StratifiedAlignmentContext;
import org.broadinstitute.sting.gatk.contexts.variantcontext.*;
import org.broadinstitute.sting.gatk.refdata.RefMetaDataTracker;
import org.broadinstitute.sting.gatk.walkers.annotator.interfaces.InfoFieldAnnotation;
import org.broadinstitute.sting.gatk.walkers.annotator.interfaces.StandardAnnotation;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;


public class QualByDepth implements InfoFieldAnnotation, StandardAnnotation {

    public Map<String, Object> annotate(RefMetaDataTracker tracker, ReferenceContext ref, Map<String, StratifiedAlignmentContext> stratifiedContexts, VariantContext vc) {
        if ( stratifiedContexts.size() == 0 )
            return null;

        final Map<String, Genotype> genotypes = vc.getGenotypes();
        if ( genotypes == null || genotypes.size() == 0 )
            return null;

        //double QbyD = genotypeQualByDepth(genotypes, stratifiedContexts);
        int qDepth = variationQualByDepth(genotypes, stratifiedContexts);
        if ( qDepth == 0 )
            return null;

        double QbyD = 10.0 * vc.getNegLog10PError() / (double)qDepth;
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(getKeyNames().get(0), String.format("%.2f", QbyD));
        return map;
    }

    public List<String> getKeyNames() { return Arrays.asList("QD"); }

    public List<VCFInfoHeaderLine> getDescriptions() { return Arrays.asList(new VCFInfoHeaderLine(getKeyNames().get(0), 1, VCFHeaderLineType.Float, "Variant Confidence/Quality by Depth")); }

    private int variationQualByDepth(final Map<String, Genotype> genotypes, Map<String, StratifiedAlignmentContext> stratifiedContexts) {
        int depth = 0;
        for ( Map.Entry<String, Genotype> genotype : genotypes.entrySet() ) {

            // we care only about variant calls
            if ( genotype.getValue().isHomRef() )
                continue;

            StratifiedAlignmentContext context = stratifiedContexts.get(genotype.getKey());
            if ( context != null )
                depth += context.getContext(StratifiedAlignmentContext.StratifiedContextType.COMPLETE).size();
        }

        return depth;
    }

//    private double genotypeQualByDepth(final Map<String, Genotype> genotypes, Map<String, StratifiedAlignmentContext> stratifiedContexts) {
//        ArrayList<Double> qualsByDepth = new ArrayList<Double>();
//        for ( Map.Entry<String, Genotype> genotype : genotypes.entrySet() ) {
//
//            // we care only about variant calls
//            if ( genotype.getValue().isHomRef() )
//                continue;
//
//            StratifiedAlignmentContext context = stratifiedContexts.get(genotype.getKey());
//            if ( context == null )
//                continue;
//
//            qualsByDepth.add(genotype.getValue().getNegLog10PError() / context.getContext(StratifiedAlignmentContext.StratifiedContextType.COMPLETE).size());
//        }
//
//        double mean = 0.0;
//        for ( Double value : qualsByDepth )
//            mean += value;
//        mean /= qualsByDepth.size();
//
//        return mean;
//    }
}