// g++ -O2 -std=c++20 kpi.cpp -lsqlite3 -o kpi

#include "sqlite3.h"
#include <cmath>
#include <string>
#include <vector>
#include <unordered_map>
#include <iostream>
#include <optional>
#include <chrono>
#include <limits>
#include <algorithm>

struct Row {
    std::string quote_date;   // YYYY-MM-DD
    std::string expiry;       // YYYY-MM-DD
    char cp;                  // 'C' or 'P'
    double S;                 // underlying/spot
    double K;                 // strike
    std::optional<double> bid;
    std::optional<double> ask;
    std::optional<double> volume;
    std::optional<double> oi;

    // derived
    int dte = 0;
    double T = 0.0;
    std::optional<double> mid;
    std::optional<double> k;      // ln(K/S)
    std::optional<double> iv;     // implied vol from mid
    std::optional<double> delta;  // BS delta using iv
};

static std::optional<double> col_double_opt(sqlite3_stmt* st, int idx) {
    if (sqlite3_column_type(st, idx) == SQLITE_NULL) return std::nullopt;
    return sqlite3_column_double(st, idx);
}

static bool parse_ymd(const std::string& s, int& y, int& m, int& d) {
    if (s.size() < 10) return false;
    y = std::stoi(s.substr(0,4));
    m = std::stoi(s.substr(5,2));
    d = std::stoi(s.substr(8,2));
    return true;
}

static int days_from_civil(int y, unsigned m, unsigned d) {
    // Gregorian calendar -> serial day number (Howard Hinnant)
    y -= (m <= 2);
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe;
}

static int days_between_ymd(const std::string& a, const std::string& b) {
    int ya, ma, da, yb, mb, db;
    if (!parse_ymd(a, ya, ma, da) || !parse_ymd(b, yb, mb, db)) return 0;
    int A = days_from_civil(ya, (unsigned)ma, (unsigned)da);
    int B = days_from_civil(yb, (unsigned)mb, (unsigned)db);
    return B - A;
}

static inline double norm_cdf(double x) {
    return 0.5 * std::erfc(-x / std::sqrt(2.0));
}

static inline double norm_pdf(double x) {
    static const double inv_sqrt_2pi = 0.39894228040143267794;
    return inv_sqrt_2pi * std::exp(-0.5 * x * x);
}

static double bs_price(char cp, double S, double K, double r, double q, double T, double vol) {
    if (T <= 0.0) {
        double intrinsic = (cp=='C') ? std::max(S-K, 0.0) : std::max(K-S, 0.0);
        return intrinsic;
    }
    vol = std::max(vol, 1e-8);
    double sqT = std::sqrt(T);
    double d1 = (std::log(S/K) + (r - q + 0.5*vol*vol)*T) / (vol*sqT);
    double d2 = d1 - vol*sqT;
    double df_r = std::exp(-r*T);
    double df_q = std::exp(-q*T);
    if (cp=='C') return df_q*S*norm_cdf(d1) - df_r*K*norm_cdf(d2);
    else         return df_r*K*norm_cdf(-d2) - df_q*S*norm_cdf(-d1);
}

static double bs_delta(char cp, double S, double K, double r, double q, double T, double vol) {
    if (T <= 0.0) {
        if (cp=='C') return (S > K) ? 1.0 : 0.0;
        return (S < K) ? -1.0 : 0.0;
    }
    vol = std::max(vol, 1e-8);
    double sqT = std::sqrt(T);
    double d1 = (std::log(S/K) + (r - q + 0.5*vol*vol)*T) / (vol*sqT);
    double df_q = std::exp(-q*T);
    if (cp=='C') return df_q * norm_cdf(d1);
    return df_q * (norm_cdf(d1) - 1.0);
}

static std::optional<double> implied_vol_bisect(char cp, double S, double K, double r, double q, double T, double price) {
    if (!(price > 0.0) || !(S > 0.0) || !(K > 0.0) || !(T > 0.0)) return std::nullopt;

    // rough no-arb bounds
    double df_r = std::exp(-r*T);
    double df_q = std::exp(-q*T);
    double lower = (cp=='C') ? std::max(df_q*S - df_r*K, 0.0) : std::max(df_r*K - df_q*S, 0.0);
    double upper = (cp=='C') ? df_q*S : df_r*K;
    if (price < lower - 1e-10 || price > upper + 1e-10) return std::nullopt;

    double lo = 1e-6, hi = 5.0; // 500% cap
    double flo = bs_price(cp,S,K,r,q,T,lo) - price;
    double fhi = bs_price(cp,S,K,r,q,T,hi) - price;

    // expand hi a bit if needed
    int expand = 0;
    while (fhi < 0.0 && hi < 10.0 && expand < 20) {
        hi *= 1.5;
        fhi = bs_price(cp,S,K,r,q,T,hi) - price;
        expand++;
    }
    if (flo > 0.0) return std::nullopt;
    if (fhi < 0.0) return std::nullopt;

    for (int it=0; it<80; ++it) {
        double mid = 0.5*(lo+hi);
        double f = bs_price(cp,S,K,r,q,T,mid) - price;
        if (std::abs(f) < 1e-10) return mid;
        if (f > 0.0) hi = mid;
        else lo = mid;
    }
    return 0.5*(lo+hi);
}

static int exec_sql(sqlite3* db, const char* sql) {
    char* err = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        std::cerr << "SQL error: " << (err?err:"") << "\n";
        sqlite3_free(err);
    }
    return rc;
}

static inline std::string key_dx(const std::string& d, const std::string& x) { return d + "|" + x; }

struct DailyExpiryAgg {
    int dte = 0;
    double atm_iv = std::numeric_limits<double>::quiet_NaN();
    double atm_abs_k = std::numeric_limits<double>::infinity();

    double call_iv = std::numeric_limits<double>::quiet_NaN();
    double call_dist = std::numeric_limits<double>::infinity();

    double put_iv = std::numeric_limits<double>::quiet_NaN();
    double put_dist = std::numeric_limits<double>::infinity();
};

int main(int argc, char** argv) {
    // Usage:
    // Untitled-1.exe options.db 0.0 0.0
    const char* db_path = (argc >= 2) ? argv[1] : "options.db";
    double r = (argc >= 3) ? std::stod(argv[2]) : 0.0;
    double q = (argc >= 4) ? std::stod(argv[3]) : 0.0;

    //const char* db_path = "options.db";   // 改成你的 db
    sqlite3* db = nullptr;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        std::cerr << "Cannot open db: " << sqlite3_errmsg(db) << "\n";
        return 1;
    }

    // You can read from raw_options or selected_options; here we assume selected_options has already been created.
    const char* sql =
        "SELECT quote_date, expiry, cp, spot, strike, bid, ask, total_volume, open_interest FROM selected_options;";

    sqlite3_stmt* st = nullptr;
    if (sqlite3_prepare_v2(db, sql, -1, &st, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare failed: " << sqlite3_errmsg(db) << "\n";
        sqlite3_close(db);
        return 1;
    }

    std::vector<Row> rows;
    rows.reserve(1'000'000);

    while (sqlite3_step(st) == SQLITE_ROW) {
        Row r0;
        r0.quote_date = reinterpret_cast<const char*>(sqlite3_column_text(st, 0));
        r0.expiry     = reinterpret_cast<const char*>(sqlite3_column_text(st, 1));
        r0.cp         = reinterpret_cast<const char*>(sqlite3_column_text(st, 2))[0];
        r0.S          = sqlite3_column_double(st, 3);
        r0.K          = sqlite3_column_double(st, 4);
        r0.bid        = col_double_opt(st, 5);
        r0.ask        = col_double_opt(st, 6);
        r0.volume     = col_double_opt(st, 7);
        r0.oi         = col_double_opt(st, 8);
        rows.push_back(std::move(r0));
    }

    sqlite3_finalize(st);

    std::cout << "Loaded rows: " << rows.size() << "\n";

    // Compute IV and KPI group A metrics
    std::unordered_map<std::string, DailyExpiryAgg> agg;
    
    for (auto& x : rows) {
        x.dte = days_between_ymd(x.quote_date, x.expiry);
        if (x.dte <= 0) continue;
        x.T = (double)x.dte / 365.0;

        if (x.S > 0.0 && x.K > 0.0) x.k = std::log(x.K / x.S);

        // mid
        if (x.bid && x.ask && *x.bid > 0.0 && *x.ask > 0.0 && *x.ask >= *x.bid) {
            x.mid = 0.5 * (*x.bid + *x.ask);
        }

        // implied vol + delta
        if (x.mid && *x.mid > 0.0) {
            x.iv = implied_vol_bisect(x.cp, x.S, x.K, r, q, x.T, *x.mid);
            if (x.iv) x.delta = bs_delta(x.cp, x.S, x.K, r, q, x.T, *x.iv);
        }

        // Aggregation: ATM IV + 25Δ skew (pick nearest in discrete strikes)
        if (x.iv && x.k && x.delta) {
            auto& a = agg[key_dx(x.quote_date, x.expiry)];
            a.dte = x.dte;

            double ak = std::abs(*x.k);
            if (ak < a.atm_abs_k) {
                a.atm_abs_k = ak;
                a.atm_iv = *x.iv;
            }

            if (x.cp=='C' && *x.k > 0.0) {
                double dist = std::abs(*x.delta - 0.25);
                if (dist < a.call_dist) { a.call_dist = dist; a.call_iv = *x.iv; }
            }
            if (x.cp=='P' && *x.k < 0.0) {
                double dist = std::abs(*x.delta + 0.25);
                if (dist < a.put_dist) { a.put_dist = dist; a.put_iv = *x.iv; }
            }
        }
    }

    // Write back to db: option_iv + daily_expiry_iv
    exec_sql(db, "BEGIN;");

    exec_sql(db,
        "DROP TABLE IF EXISTS option_iv;"
        "CREATE TABLE option_iv ("
        " quote_date TEXT NOT NULL,"
        " expiry     TEXT NOT NULL,"
        " cp         TEXT NOT NULL,"
        " spot       REAL NOT NULL,"
        " strike     REAL NOT NULL,"
        " dte        INTEGER,"
        " k          REAL,"
        " mid        REAL,"
        " iv         REAL,"
        " delta      REAL,"
        " volume     REAL,"
        " open_interest REAL,"
        " PRIMARY KEY (quote_date, expiry, cp, strike)"
        ");"
    );

    sqlite3_stmt* ins = nullptr;
    const char* ins_sql =
        "INSERT OR REPLACE INTO option_iv("
        "quote_date,expiry,cp,spot,strike,dte,k,mid,iv,delta,volume,open_interest"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?);";

    if (sqlite3_prepare_v2(db, ins_sql, -1, &ins, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare insert option_iv failed: " << sqlite3_errmsg(db) << "\n";
        exec_sql(db, "ROLLBACK;");
        sqlite3_close(db);
        return 1;
    }

    auto bind_opt = [](sqlite3_stmt* s, int idx, const std::optional<double>& v) {
        if (!v || !std::isfinite(*v)) sqlite3_bind_null(s, idx);
        else sqlite3_bind_double(s, idx, *v);
    };

    int written = 0;
    for (const auto& x : rows) {
        sqlite3_reset(ins);
        sqlite3_clear_bindings(ins);

        sqlite3_bind_text(ins, 1, x.quote_date.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins, 2, x.expiry.c_str(), -1, SQLITE_TRANSIENT);
        char cpbuf[2] = {x.cp, 0};
        sqlite3_bind_text(ins, 3, cpbuf, -1, SQLITE_TRANSIENT);

        sqlite3_bind_double(ins, 4, x.S);
        sqlite3_bind_double(ins, 5, x.K);
        sqlite3_bind_int(ins, 6, x.dte);

        bind_opt(ins, 7, x.k);
        bind_opt(ins, 8, x.mid);
        bind_opt(ins, 9, x.iv);
        bind_opt(ins,10, x.delta);
        bind_opt(ins,11, x.volume);
        bind_opt(ins,12, x.oi);

        int rc = sqlite3_step(ins);
        if (rc != SQLITE_DONE) {
            std::cerr << "Insert option_iv failed: " << sqlite3_errmsg(db) << "\n";
            sqlite3_finalize(ins);
            exec_sql(db, "ROLLBACK;");
            sqlite3_close(db);
            return 1;
        }
        written++;
    }
    sqlite3_finalize(ins);

    exec_sql(db,
        "DROP TABLE IF EXISTS daily_expiry_iv;"
        "CREATE TABLE daily_expiry_iv ("
        " quote_date TEXT NOT NULL,"
        " expiry     TEXT NOT NULL,"
        " dte        INTEGER,"
        " atm_iv     REAL,"
        " skew_25d   REAL,"
        " PRIMARY KEY (quote_date, expiry)"
        ");"
    );

    sqlite3_stmt* ins2 = nullptr;
    const char* ins2_sql =
        "INSERT OR REPLACE INTO daily_expiry_iv(quote_date,expiry,dte,atm_iv,skew_25d)"
        " VALUES (?,?,?,?,?);";

    if (sqlite3_prepare_v2(db, ins2_sql, -1, &ins2, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare insert daily_expiry_iv failed: " << sqlite3_errmsg(db) << "\n";
        exec_sql(db, "ROLLBACK;");
        sqlite3_close(db);
        return 1;
    }

    for (const auto& kv : agg) {
        const auto& key = kv.first;
        const auto& a = kv.second;
        auto pos = key.find('|');
        std::string d = (pos==std::string::npos) ? key : key.substr(0,pos);
        std::string e = (pos==std::string::npos) ? ""  : key.substr(pos+1);

        double skew = std::numeric_limits<double>::quiet_NaN();
        if (std::isfinite(a.put_iv) && std::isfinite(a.call_iv)) skew = a.put_iv - a.call_iv;

        sqlite3_reset(ins2);
        sqlite3_clear_bindings(ins2);

        sqlite3_bind_text(ins2, 1, d.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins2, 2, e.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int (ins2, 3, a.dte);

        if (std::isfinite(a.atm_iv)) sqlite3_bind_double(ins2, 4, a.atm_iv);
        else sqlite3_bind_null(ins2, 4);

        if (std::isfinite(skew)) sqlite3_bind_double(ins2, 5, skew);
        else sqlite3_bind_null(ins2, 5);

        int rc = sqlite3_step(ins2);
        if (rc != SQLITE_DONE) {
            std::cerr << "Insert daily_expiry_iv failed: " << sqlite3_errmsg(db) << "\n";
            sqlite3_finalize(ins2);
            exec_sql(db, "ROLLBACK;");
            sqlite3_close(db);
            return 1;
        }
    }
    sqlite3_finalize(ins2);

    exec_sql(db, "COMMIT;");
    sqlite3_close(db);

    std::cout << "Wrote option_iv rows: " << written << "\n";
    std::cout << "Wrote daily_expiry_iv rows: " << agg.size() << "\n";

    return 0;
}
