#include "TpcHCommons.hpp"
#include "Logger.h"
#include <getopt.h>
#include <cstring>
#include <cstdlib>
#include <istream>
#include <fstream>
#include <string>
#include <vector>
#include <ctime>
#include <chrono>
#include <sstream>
#include <iomanip>
#include <TpcHApp.hpp>

enum class CSVState {
    UnquotedField,
    QuotedField,
    QuotedQuote
};

uint8_t parsePartBrand(std::basic_string<char> &value);

uint8_t parsePartContainer(std::basic_string<char> &value);

uint8_t parseLineitemShipinstruct(std::basic_string<char> &value);

std::vector<std::string> readCSVRow(const std::string &row) {
    CSVState state = CSVState::UnquotedField;
    std::vector<std::string> fields {""};
    size_t i = 0; // index of the current field
    for (char c : row) {
        switch (state) {
            case CSVState::UnquotedField:
                switch (c) {
                    case '|': // end of field
                        fields.push_back(""); i++;
                        break;
                    case '"': state = CSVState::QuotedField;
                        break;
                    default:  fields[i].push_back(c);
                        break; }
                break;
            case CSVState::QuotedField:
                switch (c) {
                    case '"': state = CSVState::QuotedQuote;
                        break;
                    default:  fields[i].push_back(c);
                        break; }
                break;
            case CSVState::QuotedQuote:
                switch (c) {
                    case '|': // , after closing quote
                        fields.push_back(""); i++;
                        state = CSVState::UnquotedField;
                        break;
                    case '"': // "" -> "
                        fields[i].push_back('"');
                        state = CSVState::QuotedField;
                        break;
                    default:  // end of quote
                        state = CSVState::UnquotedField;
                        break; }
                break;
        }
    }
    return fields;
}

/// Read CSV file, Excel dialect. Accept "quoted fields ""with quotes"""
std::vector<std::vector<std::string>> readCSV(std::istream &in) {
    std::vector<std::vector<std::string>> table;
    std::string row;
    while (!in.eof()) {
        std::getline(in, row);
        if (in.bad() || in.fail()) {
            break;
        }
        auto fields = readCSVRow(row);
        table.push_back(fields);
    }
    return table;
}

void tpch_parse_args(int argc, char ** argv, tcph_args_t * params)
{
    int c;
    while (1) {
        static struct option long_options[] =
                {
//                        {"alg", required_argument, 0, 'a'},
//                        {"query", required_argument, 0, 'q'},
//                        {"threads", required_argument, 0, 'n'}
                };

        int option_index = 0;

        c = getopt_long(argc, argv, "a:b:m:n:q:s:",
                        long_options, &option_index);

        if (c == -1)
            break;
        switch (c) {
            case 'a':
                strcpy(params->algorithm_name, optarg);
                break;
            case 'b':
                params->bits = (uint8_t) atoi(optarg);
                break;
            case 'm':
                params->mb_restriction = atoi(optarg);
                break;
            case 'n':
                params->nthreads = (uint8_t) atoi(optarg);
                break;
            case 'q':
                params->query = (uint8_t) atoi(optarg);
                break;
            case 's':
                params->scale =(uint8_t) atoi(optarg);
                break;
            default:
                break;
        }
    }

    if (optind < argc) {
        logger(INFO, "remaining arguments: ");
        while (optind < argc) {
            logger(INFO, "%s", argv[optind++]);
        }
    }
}

uint8_t parseShipMode(const std::string& value)
{
    if (value == "MAIL") {
        return L_SHIPMODE_MAIL;
    } else if (value == "SHIP") {
        return L_SHIPMODE_SHIP;
    } else if (value == "AIR") {
        return L_SHIPMODE_AIR;
    } else if (value == "AIR REG") {
        return L_SHIPMODE_AIR_REG;
    } else {
        return 0;
    }
}

uint64_t parseDateToLong(const std::string& value)
{
    std::tm tm = {};
    std::istringstream ss(value + " 00:00:00");
    ss >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
    std::time_t time_stamp = mktime(&tm) - timezone;
    return static_cast<uint64_t>(time_stamp);
}

uint8_t parseMktSegment(const std::string& value)
{
    if (value == "BUILDING") {
        return MKT_BUILDING;
    } else {
        return 0;
    }
}

uint64_t getNumberOfLines(const std::string& filename)
{
    // find out the number of lines (tuples) in the file
    uint64_t lines = 0;
    std::string line;

    std::ifstream ifs(filename);
    while(std::getline(ifs, line))
        ++lines;
    logger(INFO, "File %s has %lu lines", filename.c_str(), lines);
    return lines;
}

std::string getPath(int scale, const std::string& tbl)
{
    std::stringstream ss;
    ss << std::setw(2) << std::setfill('0') << scale;
    return "data/scale" +  ss.str() + "/" + tbl;
}

int load_lineitem_from_file(LineItemTable *l_table, const uint8_t query, uint8_t scale)
{
    if (query != 3 && query != 10 && query != 12 && query != 19) {
        return 0;
    }

    const std::string PATH = getPath(scale, LINEITEM_TBL);

    uint64_t numTuples = getNumberOfLines(PATH);

    l_table->numTuples = numTuples;
    int rv = 0;
    rv |= posix_memalign((void **) &(l_table->l_orderkey), 64, sizeof(tuple_t) * numTuples);
    if (query == 3) {
        rv |= posix_memalign((void **) &(l_table->l_shipdate), 64, sizeof(uint64_t) * numTuples);
    } else if (query == 10) {
        rv |= posix_memalign((void **) &(l_table->l_returnflag), 64, sizeof(char) * numTuples);
    }
    else if (query == 12) {
        rv |= posix_memalign((void **) &(l_table->l_shipdate), 64, sizeof(uint64_t) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_commitdate), 64, sizeof(uint64_t) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_receiptdate), 64, sizeof(uint64_t) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_shipmode), 64, sizeof(uint8_t) * numTuples);
    } else if (query == 19) {
        rv |= posix_memalign((void **) &(l_table->l_partkey), 64, sizeof(type_key) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_quantity), 64, sizeof(float) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_shipinstruct), 64, sizeof(uint8_t) * numTuples);
        rv |= posix_memalign((void **) &(l_table->l_shipmode), 64, sizeof(uint8_t) * numTuples);
    }
    if (rv != 0) {
        logger(ERROR, "memalign error");
        return 1;
    }

    std::ifstream data(PATH);
    auto rows = readCSV(data);
    for (uint64_t i = 0; i < numTuples; i++) {
        l_table->l_orderkey[i].key = (type_key) std::stoi(rows.at(i).at(0));
        l_table->l_orderkey[i].payload = (type_value) i;
        if (query == 3) {
            l_table->l_shipdate[i] = parseDateToLong(rows.at(i).at(10));
        } else if (query == 10) {
            l_table->l_returnflag[i] = rows.at(i).at(8).c_str()[0];
        } else if (query == 12) {
            l_table->l_shipdate[i] = parseDateToLong(rows.at(i).at(10));
            l_table->l_commitdate[i] = parseDateToLong(rows.at(i).at(11));
            l_table->l_receiptdate[i] = parseDateToLong(rows.at(i).at(12));
            l_table->l_shipmode[i] = parseShipMode(rows.at(i).at(14));
        } else if (query == 19) {
            l_table->l_partkey[i] = (type_key) std::stoi(rows.at(i).at(1));
            l_table->l_quantity[i] = std::stof(rows.at(i).at(4));
            l_table->l_shipinstruct[i] = parseLineitemShipinstruct(rows.at(i).at(13));
            l_table->l_shipmode[i] = parseShipMode(rows.at(i).at(14));
        }
    }
    logger(INFO, "lineitem table loaded from file");
    return 0;
}

uint8_t parseLineitemShipinstruct(std::basic_string<char> &value)
{
    if (value == "DELIVER IN PERSON") return L_SHIPINSTRUCT_DELIVER_IN_PERSON;
    else return 0;
}

void free_lineitem(LineItemTable *l_table, const uint8_t query)
{
    if (query != 3 && query != 10 && query != 12 && query != 19) {
        return;
    }

    free(l_table->l_orderkey);
    switch (query) {
        case 3:
            free(l_table->l_shipdate);
            break;
        case 10:
            free(l_table->l_returnflag);
            break;
        case 12:
            free(l_table->l_shipdate);
            free(l_table->l_commitdate);
            free(l_table->l_receiptdate);
            free(l_table->l_shipmode);
            break;
        case 19:
            free(l_table->l_partkey);
            free(l_table->l_quantity);
            free(l_table->l_shipinstruct);
            free(l_table->l_shipmode);
            break;
        default:
            logger(ERROR, "Query %d not supported", query);
    }
}

int load_orders_from_file(OrdersTable *o_table, const uint8_t query, uint8_t scale)
{
    if (query != 3 && query != 10 && query != 12) {
        return 0;
    }

    const std::string PATH = getPath(scale, ORDERS_TBL);

    uint64_t numTuples = getNumberOfLines(PATH);

    o_table->numTuples = numTuples;
    int rv = posix_memalign((void **) &(o_table->o_orderkey), 64, sizeof(tuple_t) * numTuples);
    if (query == 3) {
        rv |= posix_memalign((void **) &(o_table->o_orderdate), 64, sizeof(uint64_t) * numTuples);
        rv |= posix_memalign((void **) &(o_table->o_custkey), 64, sizeof(type_key) * numTuples);
    } else if (query == 10) {
        rv |= posix_memalign((void **) &(o_table->o_orderdate), 64, sizeof(uint64_t) * numTuples);
        rv |= posix_memalign((void **) &(o_table->o_custkey), 64, sizeof(type_key) * numTuples);
    }
    if (rv != 0) {
        logger(ERROR, "memalign error");
        return 1;
    }
    std::ifstream data(PATH);
    auto rows = readCSV(data);
    for (uint64_t i = 0; i < numTuples; i++) {
        o_table->o_orderkey[i].key = (type_key) std::stoi(rows.at(i).at(0));
        o_table->o_orderkey[i].payload = (type_value) i;
        if (query == 3) {
            o_table->o_custkey[i] = (type_key) std::stoi(rows.at(i).at(1));
            o_table->o_orderdate[i] = parseDateToLong(rows.at(i).at(4));
        } else if (query == 10) {
            o_table->o_custkey[i] = (type_key) std::stoi(rows.at(i).at(1));
            o_table->o_orderdate[i] = parseDateToLong(rows.at(i).at(4));
        }
    }
    logger(INFO, "orders table loaded from file");
    return 0;
}

void free_orders(OrdersTable *o_table, const uint8_t query)
{
    if (query != 3 && query != 10 && query != 12) {
        return;
    }

    free(o_table->o_orderkey);
    switch (query) {
        case 3:
            free(o_table->o_custkey);
            free(o_table->o_orderdate);
            break;
        case 10:
            free(o_table->o_custkey);
            free(o_table->o_orderdate);
            break;
        default:
            break;
    }
}

int load_customer_from_file(CustomerTable *c_table, const uint8_t query, uint8_t scale)
{
    if (query != 3 && query != 10) {
        return 0;
    }
    const std::string PATH = getPath(scale, CUSTOMER_TBL);

    uint64_t numTuples = getNumberOfLines(PATH);

    c_table->numTuples = numTuples;
    int rv = posix_memalign((void **) &(c_table->c_custkey), 64, sizeof(tuple_t) * numTuples);
    if (query == 3) {
        rv |= posix_memalign((void **) &(c_table->c_mktsegment), 64, sizeof(uint8_t) * numTuples);
    } else if (query == 10) {
        rv |= posix_memalign((void **) &(c_table->c_nationkey), 64, sizeof(type_key) * numTuples);
    }

    if (rv != 0) {
        logger(ERROR, "memalign error");
        return 1;
    }
    std::ifstream data(PATH);
    auto rows = readCSV(data);
    for (uint64_t i = 0; i < numTuples; i++) {
        c_table->c_custkey[i].key = (type_key) std::stoi(rows.at(i).at(0));
        c_table->c_custkey[i].payload = (type_value) i;
        if (query == 3) c_table->c_mktsegment[i] = parseMktSegment(rows.at(i).at(6));
        if (query == 10) c_table->c_nationkey[i] = (type_key) std::stoi(rows.at(i).at(3));
    }
    logger(INFO, "customer table loaded from file");
    return 0;
}

void free_customer(CustomerTable *c_table, const uint8_t query)
{
    if (query != 3 && query != 10) {
        return;
    }

    free(c_table->c_custkey);
    if (query == 3) {
        free(c_table->c_mktsegment);
    }
    else if (query == 10) {
        free(c_table->c_nationkey);
    }
}

int load_part_from_file(PartTable *p, const uint8_t query, uint8_t scale)
{
    if (query != 19) {
        return 0;
    }

    const std::string PATH = getPath(scale, PART_TBL);

    uint64_t numTuples = getNumberOfLines(PATH);

    p->numTuples = numTuples;
    int rv = posix_memalign((void **) &(p->p_partkey), 64, sizeof(tuple_t) * numTuples);
    if (query == 19) {
        rv |= posix_memalign((void **) &(p->p_brand), 64, sizeof(uint8_t) * numTuples);
        rv |= posix_memalign((void **) &(p->p_size), 64, sizeof(uint32_t) * numTuples);
        rv |= posix_memalign((void **) &(p->p_container), 64, sizeof(uint8_t) * numTuples);
    }
    if (rv != 0) {
        logger(ERROR, "memalign error");
        return 1;
    }
    std::ifstream data(PATH);
    auto rows = readCSV(data);
    for (uint64_t i = 0; i < numTuples; i++) {
        p->p_partkey[i].key = (type_key) std::stoi(rows.at(i).at(0));
        p->p_partkey[i].payload = (type_value) i;
        if (query == 19) {
            p->p_brand[i] = parsePartBrand(rows.at(i).at(3));
            p->p_size[i] = (uint32_t) std::stoi(rows.at(i).at(5));
            p->p_container[i] = parsePartContainer(rows.at(i).at(6));
        }
    }
    logger(INFO, "part table loaded from file");
    return 0;
}

uint8_t parsePartContainer(std::basic_string<char> &value)
{
    if (value == "SM CASE") return P_CONTAINER_SM_CASE;
    else if (value == "SM BOX") return P_CONTAINER_SM_BOX;
    else if (value == "SM PACK") return P_CONTAINER_SM_PACK;
    else if (value == "SM PKG") return P_CONTAINER_SM_PKG;
    else if (value == "MED BAG") return P_CONTAINER_MED_BAG;
    else if (value == "MED BOX") return P_CONTAINER_MED_BOX;
    else if (value == "MED PKG") return P_CONTAINER_MED_PKG;
    else if (value == "MED PACK") return P_CONTAINER_MED_PACK;
    else if (value == "LG CASE") return P_CONTAINER_LG_CASE;
    else if (value == "LG BOX") return P_CONTAINER_LG_BOX;
    else if (value == "LG PACK") return P_CONTAINER_LG_PACK;
    else if (value == "LG PKG") return P_CONTAINER_LG_PKG;
    else return 0;
}

uint8_t parsePartBrand(std::basic_string<char> &value)
{
    if (value == "Brand#12") return P_BRAND_12;
    else if (value == "Brand#23") return P_BRAND_23;
    else if (value == "Brand#34") return P_BRAND_34;
    else return 0;
}

void free_part(PartTable *p, const uint8_t query)
{
    if (query != 19) {
        return;
    }

    free(p->p_partkey);
    switch (query) {
        case 19:
            free(p->p_brand);
            free(p->p_size);
            free(p->p_container);
            break;
        default:
            break;
    }
}

int load_nation_from_file(NationTable *n, uint8_t query, uint8_t scale)
{
    if (query != 10) {
        return 0;
    }

    const std::string PATH = getPath(scale, NATION_TBL);

    uint64_t numTuples = getNumberOfLines(PATH);

    n->numTuples = numTuples;
    int rv = posix_memalign((void **) &(n->n_nationkey), 64, sizeof(tuple_t) * numTuples);
    if (rv != 0) {
        logger(ERROR, "memalign error");
        return 1;
    }
    std::ifstream data(PATH);
    auto rows = readCSV(data);
    for (uint64_t i = 0; i < numTuples; i++) {
        n->n_nationkey[i].key = (type_key) std::stoi(rows.at(i).at(0));
        n->n_nationkey[i].payload = (type_value) i;
    }
    logger(INFO, "nation table loaded from file");
    return 0;
}

void free_nation(NationTable *n, uint8_t query)
{
    if (query != 10) {
        return;
    }
    free(n->n_nationkey);
}