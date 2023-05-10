#pragma once

#include <Interpreters/InDepthNodeVisitor.h>

namespace DB
{

class ASTFunction;

/// Rewrite the predicates in place
class OptimizeDateFilterWithYearInPlaceData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;
};

using OptimizeDateFilterWithYearInPlaceMatcher = OneTypeMatcher<OptimizeDateFilterWithYearInPlaceData>;
using OptimizeDateFilterWithYearInPlaceVisitor = InDepthNodeVisitor<OptimizeDateFilterWithYearInPlaceMatcher, true>;

/// Assuming that the query has been converted to DNF, try to merge toYear and other children of the AND tree
class OptimizeDateFilterWithYearInDNFData
{
public:
    using TypeToVisit = ASTFunction;
    void visit(ASTFunction & function, ASTPtr & ast) const;
};
using OptimizeDateFilterWithYearInDNFMatcher = OneTypeMatcher<OptimizeDateFilterWithYearInDNFData>;
using OptimizeDateFilterWithYearInDNFVisitor = InDepthNodeVisitor<OptimizeDateFilterWithYearInDNFMatcher, true>;

class DateFilterScanMatcher
{
public:
    struct Data
    {
        bool withToYear = false;
        bool withToYYYYMM = false;
        bool withToISOWeek = false;
    };

    static void visit(ASTPtr &, Data &);
    static bool needChildVisit(const ASTPtr &, const ASTPtr &) {return true;}
};

using DateFilterScanVisitor = InDepthNodeVisitor<DateFilterScanMatcher, true>;
}
