#include <Interpreters/OptimizeDateFilterWithYearVisitor.h>

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
    String from_date;
    String to_date;

    if (converter == "toYear")
    {
        UInt64 year = compare_to;

        from_date = date_lut.dateToString(date_lut.makeDayNum(year, 1, 1));
        to_date = date_lut.dateToString(date_lut.makeDayNum(year, 12, 31));
    }
    else if (converter == "toYYYYMM")
    {
        UInt64 year = compare_to / 100;
        UInt64 month = compare_to % 100;

        UInt8 days_of_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

        bool leap_year = (year & 3) == 0 && (year % 100 || (year % 400 == 0 && year));

        from_date = date_lut.dateToString(date_lut.makeDayNum(year, month, 1));
        to_date = date_lut.dateToString(date_lut.makeDayNum(year, month, days_of_month[month - 1] + (leap_year && month == 2)));
    }
    else
    {
        UInt64 year = compare_to / 100;
        UInt64 week = compare_to % 100;

        from_date = date_lut.dateToString(date_lut.makeDayNumFromISOWeekDate(year, week, 1));
        to_date = date_lut.dateToString(date_lut.makeDayNumFromISOWeekDate(year, week, 7));
    }

    if (comparator == "equals")
    {
        return makeASTFunction("and",
                    makeASTFunction("greaterOrEquals",
                                std::make_shared<ASTIdentifier>(column),
                                makeASTFunction("toDate", std::make_shared<ASTLiteral>(from_date))
                                ),
                    makeASTFunction("lessOrEquals",
                                std::make_shared<ASTIdentifier>(column),
                                makeASTFunction("toDate", std::make_shared<ASTLiteral>(to_date))
                                )
        );
    }
    else if (comparator == "notEquals")
    {
        return makeASTFunction("and",
                    makeASTFunction("less",
                                std::make_shared<ASTIdentifier>(column),
                                makeASTFunction("toDate", std::make_shared<ASTLiteral>(from_date))
                                ),
                    makeASTFunction("greater",
                                std::make_shared<ASTIdentifier>(column),
                                makeASTFunction("toDate", std::make_shared<ASTLiteral>(to_date))
                                )
        );
    }
    else if (comparator == "less" || comparator == "greaterOrEquals")
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    makeASTFunction("toDate", std::make_shared<ASTLiteral>(from_date))
                    );
    }
    else
    {
        return makeASTFunction(comparator,
                    std::make_shared<ASTIdentifier>(column),
                    makeASTFunction("toDate", std::make_shared<ASTLiteral>(to_date))
                    );
    }
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

    assert(function.arguments->children.size() == 2);

    size_t func_id = function.arguments->children.size();

    for (size_t i = 0; i < function.arguments->children.size(); i++)
    {
        if (const auto * func = function.arguments->children[i]->as<ASTFunction>(); func)
        {
            if (func->name == "toYear" || func->name == "toYYYYMM")
            {
                func_id = i;
            }
        }
    }

    if (func_id == function.arguments->children.size()) return false;

    size_t literal_id = 1 - func_id;

    const auto * literal = function.arguments->children[literal_id]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::UInt64) return false;

    UInt64 compare_to = literal->value.get<UInt64>();

    String comparator = literal_id > func_id ? function.name : swap_relations.at(function.name);

    const auto * func = function.arguments->children[func_id]->as<ASTFunction>();
    String column = func->arguments->children.at(0)->as<ASTIdentifier>()->name();

    ast = generateOptimizedDateFilterAST(comparator, func->name, column, compare_to);

    return true;
}

bool tryMergeToYearAndPredicate(ASTFunction & function, ASTPtr & ast, std::unordered_map<String, UInt64> & column_and_year)
{
    if (function.name != "equals") return false;

    assert(function.arguments->children.size() == 2);

    size_t func_id = function.arguments->children.size();

    for (size_t i = 0; i < function.arguments->children.size(); i++)
    {
        if (const auto * func = function.arguments->children[i]->as<ASTFunction>(); func)
        {
            if (func->name == "toISOWeek")
            {
                func_id = i;
            }
        }
    }

    if (func_id == function.arguments->children.size()) return false;

    size_t literal_id = 1 - func_id;

    const auto * literal = function.arguments->children[literal_id]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::UInt64) return false;

    UInt64 week = literal->value.get<UInt64>();

    const auto * func = function.arguments->children[func_id]->as<ASTFunction>();
    String column = func->arguments->children.at(0)->as<ASTIdentifier>()->name();

    if (!column_and_year.contains(column)) return false;

    UInt64 year = column_and_year.at(column);

    UInt64 compare_to = year * 100 + week;

    ast = generateOptimizedDateFilterAST(function.name, func->name, column, compare_to);

    column_and_year.erase(column);

    return true;
}

bool hasToYearInPredicate(const ASTPtr & predicate, UInt64 & year, String & column)
{
    auto * func = predicate->as<ASTFunction>();

    if (!func || func->name != "equals") return false;

    assert(func->arguments->children.size() == 2);

    size_t func_id = func->arguments->children.size();

    for (size_t i = 0; i < func->arguments->children.size(); ++i)
    {
        if (const auto * converter = func->arguments->children[i]->as<ASTFunction>(); converter)
        {
            if (converter->name == "toYear")
            {
                func_id = i;
                column = converter->arguments->children.at(0)->as<ASTIdentifier>()->name();
            }
        }
    }

    if (func_id == func->arguments->children.size()) return false;

    size_t literal_id = 1 - func_id;

    const auto * literal = func->arguments->children[literal_id]->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::UInt64) return false;

    year = literal->value.get<UInt64>();

    return true;
}

void OptimizeDateFilterWithYearInPlaceData::visit(ASTFunction & function, ASTPtr & ast) const
{
    rewritePredicateInPlace(function, ast);
}

void OptimizeDateFilterWithYearInDNFData::visit(ASTFunction & function, ASTPtr & ast) const
{
    if (function.name != "and")
    {
        rewritePredicateInPlace(function, ast);
        return;
    }

    std::unordered_map<String, UInt64> column_and_year;

    for (size_t i = 0; i < function.arguments->children.size(); )
    {
        UInt64 year = 0;
        String column;

        if (hasToYearInPredicate(function.arguments->children[i], year, column))
        {
            if (column_and_year.contains(column)) return;

            column_and_year.insert({column, year});
            function.arguments->children.erase(function.arguments->children.begin() + i);

            continue;
        }

        i++;
    }

    for (size_t i = 0; i < function.arguments->children.size(); i++)
    {
        auto * predicate = function.arguments->children[i]->as<ASTFunction>();
        tryMergeToYearAndPredicate(*predicate, function.arguments->children[i], column_and_year);
    }

    for (const auto & [column, year] : column_and_year)
    {
        String comparator = "equals";
        String converter = "toYear";
        function.arguments->children.push_back(generateOptimizedDateFilterAST(comparator, converter, column, year));
    }
}

void DateFilterScanMatcher::visit(ASTPtr & ast, Data & data)
{
    const auto * function = ast->as<ASTFunction>();

    if (!function) return;

    if (function->name == "toYear") data.withToYear = true;
    else if (function->name == "toYYYYMM") data.withToYYYYMM = true;
    else if (function->name == "toISOWeek") data.withToISOWeek = true;
}
}
