#include <Interpreters/OptimizeDateFilterVisitor.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

ASTPtr generateOptimizedDateFilterAST(const String & comparator, const String & converter, const String & column, UInt64 compare_to)
{
    const DateLUTImpl & date_lut = DateLUT::instance();

    String start_date;
    String end_date;

    if (converter == "toYear")
    {
        UInt64 year = compare_to;
        start_date = date_lut.dateToString(date_lut.makeDayNum(year, 1, 1));
        end_date = date_lut.dateToString(date_lut.makeDayNum(year, 12, 31));
    }
    else if (converter == "toYYYYMM")
    {
        UInt64 year = compare_to / 100;
        UInt64 month = compare_to % 100;

        if (month == 0 || month > 12) return {};

        static constexpr UInt8 days_of_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        bool leap_year = (year & 3) == 0 && (year % 100 || (year % 400 == 0 && year));

        start_date = date_lut.dateToString(date_lut.makeDayNum(year, month, 1));
        end_date = date_lut.dateToString(date_lut.makeDayNum(year, month, days_of_month[month - 1] + (leap_year && month == 2)));
    }
    else if (converter == "toYear_toISOWeek")
    {
        UInt64 year = compare_to / 100;
        UInt64 week = compare_to % 100;

        start_date = date_lut.dateToString(date_lut.makeDayNumFromISOWeekDate(year, week, 1));
        end_date = date_lut.dateToString(date_lut.makeDayNumFromISOWeekDate(year, week, 7));
    }
    else
    {
        return {};
    }

    if (comparator == "equals")
    {
        return makeASTFunction("and",
                                makeASTFunction("greaterOrEquals",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(start_date)
                                            ),
                                makeASTFunction("lessOrEquals",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(end_date)
                                            )
                                );
    }
    else if (comparator == "notEquals")
    {
        return makeASTFunction("or",
                                makeASTFunction("less",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(start_date)
                                            ),
                                makeASTFunction("greater",
                                            std::make_shared<ASTIdentifier>(column),
                                            std::make_shared<ASTLiteral>(end_date)
                                            )
                                );
    }
    else if (comparator == "less" || comparator == "greaterOrEquals")
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    std::make_shared<ASTLiteral>(start_date)
                    );
    }
    else
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    std::make_shared<ASTLiteral>(end_date)
                    );
    }
}

using ConverterNames = std::unordered_set<String>;

/// Analyze predicate in the form of "converter(column) cmp compare_to", where converter is a convert function in the set of converters_to_find,
/// column is the argument of converter, cmp is a comparison operation, which could be =, <>, <, >, <=, >=, and compare_to, a UInt64 integer,
/// is compared with the result from the converter. The order of the operands is allowed to be swapped.
bool analyzePredicate(const ASTFunction & predicate, const ConverterNames & converters_to_find, String & converter, String & column, UInt64 & compare_to, bool & converter_on_left)
{
    if (!predicate.arguments || predicate.arguments->children.size() != 2) return false;

    size_t func_id = predicate.arguments->children.size();

    for (size_t i = 0; i < predicate.arguments->children.size(); i++)
    {
        if (const auto * func = predicate.arguments->children[i]->as<ASTFunction>(); func)
        {
            if (converters_to_find.contains(func->name))
            {
                converter = func->name;
                func_id = i;
            }
        }
    }

    if (func_id == predicate.arguments->children.size()) return false;

    size_t literal_id = 1 - func_id;
    const auto * literal = predicate.arguments->children[literal_id]->as<ASTLiteral>();

    if (!literal || literal->value.getType() != Field::Types::UInt64) return false;

    compare_to = literal->value.get<UInt64>();
    converter_on_left = func_id < literal_id;

    const auto * func = predicate.arguments->children[func_id]->as<ASTFunction>();

    if (!func->arguments || func->arguments->children.size() != 1) return false;

    const auto * column_id = func->arguments->children.at(0)->as<ASTIdentifier>();

    if (!column_id) return false;

    column = column_id->name();

    return true;
}

bool rewritePredicateInPlace(ASTFunction & function, ASTPtr & ast)
{
    const static std::unordered_map<String, String> swap_relations = {
        {"equals", "equals"},
        {"notEquals", "notEquals"},
        {"less", "greater"},
        {"greater", "less"},
        {"lessOrEquals", "greaterOrEquals"},
        {"greaterOrEquals", "lessOrEquals"},
    };

    if (!swap_relations.contains(function.name)) return false;

    String column;
    String converter;
    UInt64 compare_to = 0;
    bool converter_on_left = false;

    if (!analyzePredicate(function, {"toYear", "toYYYYMM"} /*converters_to_find*/, converter, column, compare_to, converter_on_left)) return false;

    String comparator = converter_on_left ? function.name : swap_relations.at(function.name);

    const auto new_ast = generateOptimizedDateFilterAST(comparator, converter, column, compare_to);

    if (!new_ast) return false;

    ast = new_ast;
    return true;
}

using ColumnToYearAndMerged = std::unordered_map<String, std::pair<UInt64, bool>>;

bool tryMergeToYearAndPredicate(ASTPtr & ast, ColumnToYearAndMerged & column_to_year_and_merged)
{
    auto * function = ast->as<ASTFunction>();

    if (!function || function->name != "equals") return false;

    String column;
    String converter;
    UInt64 week = 0;
    bool converter_on_left = false;

    if (!analyzePredicate(*function, {"toISOWeek"} /*converters_to_find*/, converter, column, week, converter_on_left)) return false;

    if (!column_to_year_and_merged.contains(column)) return false;

    UInt64 year = column_to_year_and_merged.at(column).first;
    UInt64 compare_to = year * 100 + week;

    const auto new_ast = generateOptimizedDateFilterAST("equals" /*comparator*/, "toYear_toISOWeek" /*converter*/, column, compare_to);

    if (!new_ast) return false;

    ast = new_ast;
    column_to_year_and_merged.at(column).second = true;

    return true;
}

void OptimizeDateFilterInPlaceData::visit(ASTFunction & function, ASTPtr & ast) const
{
    rewritePredicateInPlace(function, ast);
}

void OptimizeDateFilterInDNFData::visit(ASTFunction & function, ASTPtr & ast) const
{
    if (function.name != "and")
    {
        rewritePredicateInPlace(function, ast);
        return;
    }

    ColumnToYearAndMerged column_to_year_and_merged;

    for (size_t i = 0; i < function.arguments->children.size();)
    {
        String column;
        String converter;
        UInt64 year = 0;
        bool converter_on_left = false;

        auto * predicate = function.arguments->children[i]->as<ASTFunction>();

        if (predicate && predicate->name == "equals"
            && analyzePredicate(*predicate, {"toYear"} /*converters_to_find*/, converter, column, year, converter_on_left))
        {
            if (column_to_year_and_merged.contains(column)) return;

            column_to_year_and_merged.insert({column, std::make_pair(year, false)});
            function.arguments->children.erase(function.arguments->children.begin() + i);
        }
        else i++;
    }

    if (column_to_year_and_merged.empty()) return;

    for (size_t i = 0; i < function.arguments->children.size(); i++)
    {
        tryMergeToYearAndPredicate(function.arguments->children[i], column_to_year_and_merged);
    }

    for (const auto & [column, year_and_merged] : column_to_year_and_merged)
    {
        if (!year_and_merged.second)
            function.arguments->children.push_back(generateOptimizedDateFilterAST("equals" /*comparator*/, "toYear" /*converter*/, column, year_and_merged.first));
    }
}

void ScanMergeablesInDateFilterMatcher::visit(ASTPtr & ast, Data & data)
{
    const auto * function = ast->as<ASTFunction>();

    if (!function) return;

    if (function->name == "toYear") data.has_toYear = true;
    else if (function->name == "toISOWeek") data.has_toISOWeek = true;
}

}
