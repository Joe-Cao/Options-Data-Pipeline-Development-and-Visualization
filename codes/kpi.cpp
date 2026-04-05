// g++ -O2 -std=c++20 kpi.cpp -lsqlite3 -o kpi
// cl /EHsc /std:c++20 kpi.cpp sqlite3.c /Fe:kpi.exe

#include "sqlite3.h"
#include <cmath>
#include <string>
#include <vector>
#include <unordered_map>
#include <map>
#include <iostream>
#include <optional>
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

    // A) + IV
    int dte = 0;
    double T = 0.0;
    std::optional<double> mid;
    std::optional<double> k;      // ln(K/S)
    std::optional<double> iv;     // implied vol from mid
    std::optional<double> delta;  // BS delta using iv

    // B) liquidity / spread
    std::optional<double> spread_abs; // ask-bid
    std::optional<double> rel_spread; // (ask-bid)/mid
    std::optional<double> logv;       // log(volume+1)
    std::optional<double> logo;       // log(oi+1)
    std::optional<double> z_rel_spread;
    std::optional<double> z_logv;
    std::optional<double> z_logo;
    std::optional<double> liq_score;  // z(-rel_spread)+z(logv)+z(logo)

    // C) anomalies flags
    std::optional<double> z_iv_group;
    std::optional<double> z_spread_group;
    std::optional<double> z_iv_local;
    int is_extreme_iv_group = 0;
    int is_extreme_spread_group = 0;
    int is_local_iv_outlier = 0;
    int is_spread_liq_outlier = 0; // large spread but volume/OI present
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

static int ymd_to_serial(const std::string& s) {
    int y,m,d;
    if (!parse_ymd(s,y,m,d)) return 0;
    return days_from_civil(y,(unsigned)m,(unsigned)d);
}

static inline double norm_cdf(double x) {
    return 0.5 * std::erfc(-x / std::sqrt(2.0));
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

    double df_r = std::exp(-r*T);
    double df_q = std::exp(-q*T);
    double lower = (cp=='C') ? std::max(df_q*S - df_r*K, 0.0) : std::max(df_r*K - df_q*S, 0.0);
    double upper = (cp=='C') ? df_q*S : df_r*K;
    if (price < lower - 1e-10 || price > upper + 1e-10) return std::nullopt;

    double lo = 1e-6, hi = 5.0;
    double flo = bs_price(cp,S,K,r,q,T,lo) - price;
    double fhi = bs_price(cp,S,K,r,q,T,hi) - price;

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
static inline std::string key_dxc(const std::string& d, const std::string& x, char cp) {
    return d + "|" + x + "|" + std::string(1, cp);
}

struct DailyExpiryAgg {
    int dte = 0;
    double atm_iv = std::numeric_limits<double>::quiet_NaN();
    double atm_abs_k = std::numeric_limits<double>::infinity();

    double call_iv = std::numeric_limits<double>::quiet_NaN();
    double call_dist = std::numeric_limits<double>::infinity();

    double put_iv = std::numeric_limits<double>::quiet_NaN();
    double put_dist = std::numeric_limits<double>::infinity();

    long long n_spread = 0;
    double sum_rel_spread = 0.0;
    long long n_liq = 0;
    double sum_liq = 0.0;

    void add_rel_spread(double x) { n_spread++; sum_rel_spread += x; }
    void add_liq(double x) { n_liq++; sum_liq += x; }
};

struct StatsAcc {
    long long n = 0;
    double sum = 0.0;
    double sumsq = 0.0;
    void add(double x) { n++; sum += x; sumsq += x*x; }
    bool ok() const { return n >= 2; }
    double mean() const { return sum / (double)n; }
    double var() const {
        double m = mean();
        double v = sumsq/(double)n - m*m;
        return (v > 0.0) ? v : 0.0;
    }
    double sd() const { return std::sqrt(var()); }
};

static std::optional<double> z_of(const StatsAcc& a, double x) {
    if (!a.ok()) return std::nullopt;
    double s = a.sd();
    if (s <= 1e-12) return std::nullopt;
    return (x - a.mean()) / s;
}

int main(int argc, char** argv) {
    const char* db_path = (argc >= 2) ? argv[1] : "options.db";
    double r = (argc >= 3) ? std::stod(argv[2]) : 0.0;
    double q = (argc >= 4) ? std::stod(argv[3]) : 0.0;

    sqlite3* db = nullptr;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        std::cerr << "Cannot open db: " << sqlite3_errmsg(db) << "\n";
        return 1;
    }

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

    std::unordered_map<std::string, DailyExpiryAgg> agg;

    std::unordered_map<std::string, StatsAcc> acc_rel_spread_by_date;
    std::unordered_map<std::string, StatsAcc> acc_logv_by_date;
    std::unordered_map<std::string, StatsAcc> acc_logo_by_date;

    std::unordered_map<std::string, StatsAcc> acc_iv_by_dx;
    std::unordered_map<std::string, StatsAcc> acc_spread_by_dx;

    // Pass1
    for (auto& x : rows) {
        x.dte = days_between_ymd(x.quote_date, x.expiry);
        if (x.dte <= 0) continue;
        x.T = (double)x.dte / 365.0;

        if (x.S > 0.0 && x.K > 0.0) x.k = std::log(x.K / x.S);

        if (x.bid && x.ask && *x.bid > 0.0 && *x.ask > 0.0 && *x.ask >= *x.bid) {
            x.mid = 0.5 * (*x.bid + *x.ask);
            x.spread_abs = *x.ask - *x.bid;
            if (*x.mid > 0.0) x.rel_spread = (*x.ask - *x.bid) / (*x.mid);
        }

        if (x.volume) x.logv = std::log(*x.volume + 1.0);
        if (x.oi)     x.logo = std::log(*x.oi + 1.0);

        if (x.mid && *x.mid > 0.0) {
            x.iv = implied_vol_bisect(x.cp, x.S, x.K, r, q, x.T, *x.mid);
            if (x.iv) x.delta = bs_delta(x.cp, x.S, x.K, r, q, x.T, *x.iv);
        }

        // A aggregation
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

        // B stats by date
        if (x.rel_spread && std::isfinite(*x.rel_spread)) acc_rel_spread_by_date[x.quote_date].add(*x.rel_spread);
        if (x.logv && std::isfinite(*x.logv))             acc_logv_by_date[x.quote_date].add(*x.logv);
        if (x.logo && std::isfinite(*x.logo))             acc_logo_by_date[x.quote_date].add(*x.logo);

        // C group stats by date+expiry
        const auto kdx = key_dx(x.quote_date, x.expiry);
        if (x.iv && std::isfinite(*x.iv))                 acc_iv_by_dx[kdx].add(*x.iv);
        if (x.rel_spread && std::isfinite(*x.rel_spread)) acc_spread_by_dx[kdx].add(*x.rel_spread);
    }

    // Pass2
    for (auto& x : rows) {
        if (x.dte <= 0) continue;

        if (x.rel_spread) x.z_rel_spread = z_of(acc_rel_spread_by_date[x.quote_date], *x.rel_spread);
        if (x.logv)       x.z_logv       = z_of(acc_logv_by_date[x.quote_date], *x.logv);
        if (x.logo)       x.z_logo       = z_of(acc_logo_by_date[x.quote_date], *x.logo);

        double score = 0.0;
        bool any = false;
        if (x.z_rel_spread) { score += -(*x.z_rel_spread); any = true; }
        if (x.z_logv)       { score +=  (*x.z_logv);       any = true; }
        if (x.z_logo)       { score +=  (*x.z_logo);       any = true; }
        if (any) x.liq_score = score;

        const auto kdx = key_dx(x.quote_date, x.expiry);

        if (x.iv) {
            x.z_iv_group = z_of(acc_iv_by_dx[kdx], *x.iv);
            if (x.z_iv_group && std::abs(*x.z_iv_group) > 3.0) x.is_extreme_iv_group = 1;
        }
        if (x.rel_spread) {
            x.z_spread_group = z_of(acc_spread_by_dx[kdx], *x.rel_spread);
            if (x.z_spread_group && std::abs(*x.z_spread_group) > 3.0) x.is_extreme_spread_group = 1;
        }

        if (x.rel_spread && *x.rel_spread > 0.25) {
            double v = x.volume.value_or(0.0);
            double oi = x.oi.value_or(0.0);
            if (v > 0.0 || oi > 100.0) x.is_spread_liq_outlier = 1;
        }

        auto itAgg = agg.find(kdx);
        if (itAgg != agg.end()) {
            if (x.rel_spread) itAgg->second.add_rel_spread(*x.rel_spread);
            if (x.liq_score)  itAgg->second.add_liq(*x.liq_score);
        }
    }

    // Pass3: local IV outliers
    std::unordered_map<std::string, std::vector<int>> idx_by_dxc;
    for (int i=0;i<(int)rows.size();++i) {
        const auto& x = rows[i];
        if (x.dte <= 0) continue;
        if (!(x.iv && x.k)) continue;
        idx_by_dxc[key_dxc(x.quote_date, x.expiry, x.cp)].push_back(i);
    }

    for (auto& kv : idx_by_dxc) {
        auto& idxs = kv.second;
        if ((int)idxs.size() < 9) continue;
        std::sort(idxs.begin(), idxs.end(), [&](int a, int b){ return *(rows[a].k) < *(rows[b].k); });

        for (int j=2; j<(int)idxs.size()-2; ++j) {
            int id = idxs[j];
            double iv0 = *(rows[id].iv);
            double sum = 0.0, sumsq = 0.0; int n=0;
            for (int t=j-2; t<=j+2; ++t) {
                if (t==j) continue;
                double ivt = *(rows[idxs[t]].iv);
                sum += ivt; sumsq += ivt*ivt; n++;
            }
            double mean = sum / n;
            double var = sumsq / n - mean*mean;
            double sd = (var > 1e-12) ? std::sqrt(var) : 0.0;
            if (sd <= 1e-6) continue;
            double z = (iv0 - mean) / sd;
            rows[id].z_iv_local = z;
            if (std::abs(z) > 3.0) rows[id].is_local_iv_outlier = 1;
        }
    }

    // ===== Write back to DB =====
    exec_sql(db, "BEGIN;");

    exec_sql(db,
        "DROP TABLE IF EXISTS option_kpi;"
        "CREATE TABLE option_kpi ("
        " quote_date TEXT NOT NULL,"
        " expiry     TEXT NOT NULL,"
        " cp         TEXT NOT NULL,"
        " spot       REAL NOT NULL,"
        " strike     REAL NOT NULL,"
        " dte        INTEGER,"
        " k          REAL,"
        " bid        REAL,"
        " ask        REAL,"
        " mid        REAL,"
        " spread_abs REAL,"
        " rel_spread REAL,"
        " volume     REAL,"
        " open_interest REAL,"
        " iv         REAL,"
        " delta      REAL,"
        " logv       REAL,"
        " logo       REAL,"
        " z_rel_spread REAL,"
        " z_logv       REAL,"
        " z_logo       REAL,"
        " liq_score    REAL,"
        " z_iv_group   REAL,"
        " z_spread_group REAL,"
        " z_iv_local   REAL,"
        " is_extreme_iv_group INTEGER,"
        " is_extreme_spread_group INTEGER,"
        " is_local_iv_outlier INTEGER,"
        " is_spread_liq_outlier INTEGER,"
        " PRIMARY KEY (quote_date, expiry, cp, strike)"
        ");"
    );

    sqlite3_stmt* ins = nullptr;
    const char* ins_sql =
        "INSERT OR REPLACE INTO option_kpi("
        "quote_date,expiry,cp,spot,strike,dte,k,bid,ask,mid,spread_abs,rel_spread,volume,open_interest,"
        "iv,delta,logv,logo,z_rel_spread,z_logv,z_logo,liq_score,z_iv_group,z_spread_group,z_iv_local,"
        "is_extreme_iv_group,is_extreme_spread_group,is_local_iv_outlier,is_spread_liq_outlier"
        ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    if (sqlite3_prepare_v2(db, ins_sql, -1, &ins, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare insert option_kpi failed: " << sqlite3_errmsg(db) << "\n";
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
        bind_opt(ins, 8, x.bid);
        bind_opt(ins, 9, x.ask);
        bind_opt(ins,10, x.mid);
        bind_opt(ins,11, x.spread_abs);
        bind_opt(ins,12, x.rel_spread);
        bind_opt(ins,13, x.volume);
        bind_opt(ins,14, x.oi);
        bind_opt(ins,15, x.iv);
        bind_opt(ins,16, x.delta);
        bind_opt(ins,17, x.logv);
        bind_opt(ins,18, x.logo);
        bind_opt(ins,19, x.z_rel_spread);
        bind_opt(ins,20, x.z_logv);
        bind_opt(ins,21, x.z_logo);
        bind_opt(ins,22, x.liq_score);
        bind_opt(ins,23, x.z_iv_group);
        bind_opt(ins,24, x.z_spread_group);
        bind_opt(ins,25, x.z_iv_local);

        sqlite3_bind_int(ins,26, x.is_extreme_iv_group);
        sqlite3_bind_int(ins,27, x.is_extreme_spread_group);
        sqlite3_bind_int(ins,28, x.is_local_iv_outlier);
        sqlite3_bind_int(ins,29, x.is_spread_liq_outlier);

        int rc = sqlite3_step(ins);
        if (rc != SQLITE_DONE) {
            std::cerr << "Insert option_kpi failed: " << sqlite3_errmsg(db) << "\n";
            sqlite3_finalize(ins);
            exec_sql(db, "ROLLBACK;");
            sqlite3_close(db);
            return 1;
        }
        written++;
    }
    sqlite3_finalize(ins);

    exec_sql(db,
        "DROP TABLE IF EXISTS daily_expiry_kpi;"
        "CREATE TABLE daily_expiry_kpi ("
        " quote_date TEXT NOT NULL,"
        " expiry     TEXT NOT NULL,"
        " dte        INTEGER,"
        " atm_iv     REAL,"
        " skew_25d   REAL,"
        " avg_rel_spread REAL,"
        " avg_liq_score REAL,"
        " dskew      REAL,"
        " z_dskew    REAL,"
        " skew_jump_flag INTEGER,"
        " PRIMARY KEY (quote_date, expiry)"
        ");"
    );

    // skew jump calc
    struct SkewPt { int t; std::string date; double skew; double dskew=std::numeric_limits<double>::quiet_NaN(); double z=std::numeric_limits<double>::quiet_NaN(); int flag=0; };
    std::unordered_map<std::string, std::vector<SkewPt>> skew_by_expiry;

    for (const auto& kv : agg) {
        const auto& key = kv.first;
        const auto& a = kv.second;
        auto pos = key.find('|');
        std::string d = (pos==std::string::npos) ? key : key.substr(0,pos);
        std::string e = (pos==std::string::npos) ? ""  : key.substr(pos+1);
        double skew = std::numeric_limits<double>::quiet_NaN();
        if (std::isfinite(a.put_iv) && std::isfinite(a.call_iv)) skew = a.put_iv - a.call_iv;
        if (!std::isfinite(skew)) continue;
        skew_by_expiry[e].push_back(SkewPt{ymd_to_serial(d), d, skew});
    }

    for (auto& kv : skew_by_expiry) {
        auto& v = kv.second;
        std::sort(v.begin(), v.end(), [](const SkewPt& a, const SkewPt& b){ return a.t < b.t; });
        for (size_t i=1;i<v.size();++i) v[i].dskew = v[i].skew - v[i-1].skew;

        const int W = 20;
        for (size_t i=1;i<v.size();++i) {
            int start = (int)((i> (size_t)W) ? (i-W) : 1);
            StatsAcc a;
            for (int j=start;j<=(int)i;++j) if (std::isfinite(v[j].dskew)) a.add(v[j].dskew);
            if (a.ok() && std::isfinite(v[i].dskew)) {
                auto z = z_of(a, v[i].dskew);
                if (z) { v[i].z = *z; if (std::abs(*z) > 3.0) v[i].flag = 1; }
            }
        }
    }

    std::unordered_map<std::string, SkewPt> skew_jump;
    for (const auto& kv : skew_by_expiry) {
        const auto& expiry = kv.first;
        for (const auto& p : kv.second) skew_jump[key_dx(p.date, expiry)] = p;
    }

    sqlite3_stmt* ins2 = nullptr;
    const char* ins2_sql =
        "INSERT OR REPLACE INTO daily_expiry_kpi(quote_date,expiry,dte,atm_iv,skew_25d,avg_rel_spread,avg_liq_score,dskew,z_dskew,skew_jump_flag)"
        " VALUES (?,?,?,?,?,?,?,?,?,?);";

    if (sqlite3_prepare_v2(db, ins2_sql, -1, &ins2, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare insert daily_expiry_kpi failed: " << sqlite3_errmsg(db) << "\n";
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

        double avg_rel_spread = (a.n_spread>0) ? (a.sum_rel_spread / (double)a.n_spread) : std::numeric_limits<double>::quiet_NaN();
        double avg_liq = (a.n_liq>0) ? (a.sum_liq / (double)a.n_liq) : std::numeric_limits<double>::quiet_NaN();

        auto sjit = skew_jump.find(key_dx(d,e));
        double dskew = std::numeric_limits<double>::quiet_NaN();
        double z_dskew = std::numeric_limits<double>::quiet_NaN();
        int flag = 0;
        if (sjit != skew_jump.end()) { dskew = sjit->second.dskew; z_dskew = sjit->second.z; flag = sjit->second.flag; }

        sqlite3_reset(ins2);
        sqlite3_clear_bindings(ins2);

        sqlite3_bind_text(ins2, 1, d.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins2, 2, e.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_int (ins2, 3, a.dte);

        if (std::isfinite(a.atm_iv)) sqlite3_bind_double(ins2, 4, a.atm_iv); else sqlite3_bind_null(ins2, 4);
        if (std::isfinite(skew))     sqlite3_bind_double(ins2, 5, skew);     else sqlite3_bind_null(ins2, 5);
        if (std::isfinite(avg_rel_spread)) sqlite3_bind_double(ins2, 6, avg_rel_spread); else sqlite3_bind_null(ins2, 6);
        if (std::isfinite(avg_liq)) sqlite3_bind_double(ins2, 7, avg_liq); else sqlite3_bind_null(ins2, 7);
        if (std::isfinite(dskew)) sqlite3_bind_double(ins2, 8, dskew); else sqlite3_bind_null(ins2, 8);
        if (std::isfinite(z_dskew)) sqlite3_bind_double(ins2, 9, z_dskew); else sqlite3_bind_null(ins2, 9);
        sqlite3_bind_int(ins2, 10, flag);

        int rc = sqlite3_step(ins2);
        if (rc != SQLITE_DONE) {
            std::cerr << "Insert daily_expiry_kpi failed: " << sqlite3_errmsg(db) << "\n";
            sqlite3_finalize(ins2);
            exec_sql(db, "ROLLBACK;");
            sqlite3_close(db);
            return 1;
        }
    }
    sqlite3_finalize(ins2);

    exec_sql(db,
        "DROP TABLE IF EXISTS anomalies;"
        "CREATE TABLE anomalies("
        " quote_date TEXT NOT NULL,"
        " expiry     TEXT NOT NULL,"
        " cp         TEXT NOT NULL,"
        " strike     REAL NOT NULL,"
        " spot       REAL NOT NULL,"
        " kind       TEXT NOT NULL,"
        " iv         REAL,"
        " rel_spread REAL,"
        " z_iv_group REAL,"
        " z_spread_group REAL,"
        " z_iv_local REAL,"
        " liq_score REAL,"
        " volume REAL,"
        " open_interest REAL,"
        " PRIMARY KEY (quote_date, expiry, cp, strike, kind)"
        ");"
    );

    sqlite3_stmt* ins3 = nullptr;
    const char* ins3_sql =
        "INSERT OR REPLACE INTO anomalies(quote_date,expiry,cp,strike,spot,kind,iv,rel_spread,z_iv_group,z_spread_group,z_iv_local,liq_score,volume,open_interest)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);";

    if (sqlite3_prepare_v2(db, ins3_sql, -1, &ins3, nullptr) != SQLITE_OK) {
        std::cerr << "Prepare insert anomalies failed: " << sqlite3_errmsg(db) << "\n";
        exec_sql(db, "ROLLBACK;");
        sqlite3_close(db);
        return 1;
    }

    int aw = 0;
    auto insert_anom = [&](const Row& x, const char* kind){
        sqlite3_reset(ins3);
        sqlite3_clear_bindings(ins3);
        sqlite3_bind_text(ins3, 1, x.quote_date.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(ins3, 2, x.expiry.c_str(), -1, SQLITE_TRANSIENT);
        char cpbuf[2] = {x.cp, 0};
        sqlite3_bind_text(ins3, 3, cpbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_double(ins3, 4, x.K);
        sqlite3_bind_double(ins3, 5, x.S);
        sqlite3_bind_text(ins3, 6, kind, -1, SQLITE_TRANSIENT);

        bind_opt(ins3, 7, x.iv);
        bind_opt(ins3, 8, x.rel_spread);
        bind_opt(ins3, 9, x.z_iv_group);
        bind_opt(ins3,10, x.z_spread_group);
        bind_opt(ins3,11, x.z_iv_local);
        bind_opt(ins3,12, x.liq_score);
        bind_opt(ins3,13, x.volume);
        bind_opt(ins3,14, x.oi);

        if (sqlite3_step(ins3) == SQLITE_DONE) aw++;
    };

    for (const auto& x : rows) {
        if (x.is_extreme_iv_group) insert_anom(x, "EXTREME_IV");
        if (x.is_extreme_spread_group) insert_anom(x, "EXTREME_SPREAD");
        if (x.is_local_iv_outlier) insert_anom(x, "LOCAL_IV_OUTLIER");
        if (x.is_spread_liq_outlier) insert_anom(x, "SPREAD_WITH_ACTIVITY");
    }
    sqlite3_finalize(ins3);

    exec_sql(db, "COMMIT;");
    sqlite3_close(db);

    std::cout << "Wrote option_kpi rows: " << written << "\n";
    std::cout << "Wrote daily_expiry_kpi rows: " << agg.size() << "\n";
    std::cout << "Wrote anomalies rows: " << aw << "\n";
    return 0;
}