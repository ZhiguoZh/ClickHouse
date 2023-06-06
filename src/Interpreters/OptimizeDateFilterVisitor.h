#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite the predicates in place
class OptimizeDateFilterInPlaceData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;
};

using OptimizeDateFilterInPlaceMatcher = OneTypeMatcher<OptimizeDateFilterInPlaceData>;
using OptimizeDateFilterInPlaceVisitor = InDepthNodeVisitor<OptimizeDateFilterInPlaceMatcher, true>;

/// Find mergeable predicates joined by AND, rewrite them to avoid date converters.
/// This visitor works under the condition that the query has been converted to its DNF.
class OptimizeDateFilterInDNFData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;
};
using OptimizeDateFilterInDNFMatcher = OneTypeMatcher<OptimizeDateFilterInDNFData>;
using OptimizeDateFilterInDNFVisitor = InDepthNodeVisitor<OptimizeDateFilterInDNFMatcher, true>;

class ScanMergeablesInDateFilterMatcher
{
public:
    struct Data
    {
        bool has_toYear = false;
        bool has_toISOWeek = false;
    };

    static void visit(ASTPtr &, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) {return true;}
};
using ScanMergeablesInDateFilterVisitor = InDepthNodeVisitor<ScanMergeablesInDateFilterMatcher, true>;

}
