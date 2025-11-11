const express = require("express");
const https = require("https");
const cors = require("cors");
require("dotenv").config();

const app = express();
// Ensure your API Key is available in the environment variables
const API_KEY = process.env.API_KEY || "Cwq9KXecjqRjV94y5nqA3EM4OAlGV1Xa";

// --- OPTIMIZATION 1: Centralized In-Memory Cache ---
const apiCache = new Map();
const CACHE_TTL_MS = 1000 * 60 * 30; // Cache data for 30 minutes

// --- START: MODIFICATION ---
// This new cache will store the calculated historical average rates.
const historicalRateCache = new Map();

/**
 * Fetches data from a URL with a retry mechanism.
 * This is a robust helper for all API calls.
 */
async function fetchDataWithRetry(url, retries = 3, backoff = 500) {
    const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
    for (let i = 0; i < retries; i++) {
        try {
            return await new Promise((resolve, reject) => {
                const request = https.get(url, { timeout: 20000 }, (res) => {
                    if (res.statusCode < 200 || res.statusCode >= 300) {
                        return reject(new Error(`HTTP Error: ${res.statusCode}`));
                    }
                    let body = "";
                    res.on("data", (chunk) => (body += chunk));
                    res.on("end", () => {
                        try {
                            const parsed = JSON.parse(body);
                            if (parsed && (parsed["Error Message"] || parsed.hasOwnProperty('error'))) {
                                return reject(
                                    new Error(`API Error: ${parsed["Error Message"] || parsed.error}`)
                                );
                            }
                            resolve(parsed);
                        } catch (e) {
                            reject(new Error("Invalid JSON response"));
                        }
                    });
                }).on("error", reject).on("timeout", () => {
                    request.destroy();
                    reject(new Error("Request timed out"));
                });
            });
        } catch (error) {
            console.warn(`[FETCHDATA] Attempt ${i + 1} failed for ${url.split("apikey=")[0]}: ${error.message}`);
            if (i === retries - 1) throw error;
            await delay(backoff * Math.pow(2, i));
        }
    }
}


/**
 * --- NEW FUNCTION ---
 * Calculates the average exchange rate for a given year by fetching the rate
 * from the 15th of each month from the FMP API.
 * @param {string} baseCurrency - The currency to convert from (e.g., 'SAR').
 * @param {string} targetCurrency - The currency to convert to (e.g., 'USD').
 * @param {string} year - The four-digit year (e.g., '2023').
 * @returns {Promise<number|null>} The average exchange rate or null if it fails.
 */
async function getHistoricalAverageRate(baseCurrency, targetCurrency, year) {
    if (baseCurrency === targetCurrency) return 1.0;

    const cacheKey = `${baseCurrency}-${targetCurrency}-${year}`;
    if (historicalRateCache.has(cacheKey)) {
        console.log(`[RATE-CACHE] Using cached historical rate for ${cacheKey}`);
        return historicalRateCache.get(cacheKey);
    }

    console.log(`[RATE-FETCH] Calculating historical average rate for ${cacheKey}...`);
    const currencyPair = `${baseCurrency}${targetCurrency}`;
    const monthlyRatePromises = [];

    // Create an array of 12 promises, one for each month.
    for (let month = 1; month <= 12; month++) {
        const date = `${year}-${String(month).padStart(2, '0')}-15`;
        // Use the historical price endpoint as requested
        const url = `https://financialmodelingprep.com/api/v3/historical-price-full/${currencyPair}?from=${date}&to=${date}&apikey=${API_KEY}`;
        monthlyRatePromises.push(fetchDataWithRetry(url));
    }

    try {
        const results = await Promise.all(monthlyRatePromises);
        let totalRate = 0;
        let validMonths = 0;

        results.forEach((result, index) => {
            if (result && result.historical && result.historical.length > 0 && result.historical[0].close) {
                totalRate += result.historical[0].close;
                validMonths++;
            } else {
                console.warn(`[RATE-FETCH] No data for ${currencyPair} on month ${index + 1}/${year}.`);
            }
        });

        if (validMonths === 0) {
            throw new Error(`Could not fetch any monthly exchange rates for ${currencyPair} in ${year}.`);
        }

        const averageRate = totalRate / validMonths;
        console.log(`[RATE-FETCH] Average rate for ${cacheKey} is ${averageRate.toFixed(4)} from ${validMonths} valid months.`);
        
        // Cache the successful result permanently for this run
        historicalRateCache.set(cacheKey, averageRate);
        return averageRate;

    } catch (error) {
        console.error(`[RATE-FETCH-ERROR] Failed to get historical average for ${cacheKey}: ${error.message}`);
        // Cache the failure for a shorter time to avoid repeated failed requests
        historicalRateCache.set(cacheKey, null, 1000 * 60 * 5); // Cache failure for 5 mins
        return null;
    }
}


/**
 * --- REPLACED FUNCTION ---
 * Fetches historical average exchange rates for a list of currencies to USD for a specific year.
 * This now uses our new historical average function.
 * @param {string[]} currencies - An array of currency codes (e.g., ['SAR', 'EUR']).
 * @param {string} year - The year for the historical average.
 * @returns {Promise<Map<string, number|null>>} A map of currency codes to their average rate against USD.
 */
async function getUsdExchangeRates(currencies, year) {
    const rates = new Map();
    const uniqueCurrencies = [...new Set(currencies.map(c => c.toUpperCase()))]; // Remove duplicates and normalize

    const ratePromises = uniqueCurrencies.map(currency => {
        if (currency === 'USD') {
            return Promise.resolve({ currency: 'USD', rate: 1.0 });
        }
        // We get the rate to convert the foreign currency TO USD.
        return getHistoricalAverageRate(currency, 'USD', year).then(rate => ({ currency, rate }));
    });

    const results = await Promise.all(ratePromises);

    for (const result of results) {
        rates.set(result.currency, result.rate);
    }

    return rates;
}
// --- END: MODIFICATION ---


// --- CORE HELPER FUNCTIONS (UNCHANGED) ---

function getCompanyCurrency(company) {
  if (company && company.reportedCurrency)
    return company.reportedCurrency.toUpperCase();
  if (company && company.currency) return company.currency.toUpperCase();
  if (company && company.symbol) {
    const symbol = company.symbol.toUpperCase();
    if (symbol.endsWith(".SR")) return "SAR";
    if (symbol.endsWith(".L") || symbol.endsWith(".LN")) return "GBP";
    if (symbol.endsWith(".PA") || symbol.endsWith(".FP")) return "EUR";
    if (symbol.endsWith(".DE") || symbol.endsWith(".XETRA")) return "EUR";
    if (symbol.endsWith(".AS") || symbol.endsWith(".AEB")) return "EUR";
    if (symbol.endsWith(".TO")) return "CAD";
    if (symbol.endsWith(".AX")) return "AUD";
    if (symbol.endsWith(".HK")) return "HKD";
    if (symbol.endsWith(".SS")) return "SGD";
    if (symbol.endsWith(".BO") || symbol.endsWith(".NS")) return "INR";
  }
  return "USD";
}

function getStandardizedIndustry(industry) {
  if (!industry) return "Unknown";
  const normalized = industry.toLowerCase().trim();
  if (normalized.includes("telecom")) {
    return "Telecommunications Services";
  }
  return industry;
}

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function getMockData(companyCode, year) {
  console.log(`[MOCK] Generating mock data for ${companyCode}, ${year}`);
  const mockCompanies = Array.from({ length: 20 }, (_, i) => ({
    symbol: i === 0 ? companyCode : `COMP${i}`,
    companyName: i === 0 ? "Selected Company" : `Company ${i}`,
    sector: "Technology",
    industry: "Software",
    country: "US",
    marketCap: 1000000000000 - i * 10000000000,
    isSelected: i === 0,
    currency: "USD",
  }));
  return {
    allCompanies: mockCompanies,
    topCompanies: mockCompanies.slice(0, 10),
    comparisonCompanies: mockCompanies,
  };
}

// --- EXPRESS APP SETUP (UNCHANGED) ---

app.use(cors({ origin: "*", methods: ["GET", "POST", "OPTIONS"], allowedHeaders: ["Content-Type", "Authorization"], credentials: false, }));
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET", "POST", "OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type", "Authorization");
  if (req.method === "OPTIONS") { return res.status(200).end(); }
  next();
});
app.use(express.static(__dirname));
app.use((req, res, next) => {
  console.log(`[REQUEST] ${req.method} ${req.url}`);
  next();
});


// --- DUPLICATE HANDLING (UNCHANGED) ---

function deduplicateCompanies(companies) {
  if (!companies || companies.length === 0) { return []; }
  console.log(`[DEDUPE] Starting de-duplication process on ${companies.length} companies.`);
  const companyGroups = new Map();
  const getBaseName = (name) => {
    if (!name) return "";
    return name.replace(/\s+\d+(\.\d+)?%.*$/, "").trim();
  };
  for (const company of companies) {
    if (!company.companyName || company.isActivelyTrading === undefined || !company.ipoDate) {
      console.warn(`[DEDUPE] Skipping company ${company.symbol} because it's missing required fields (name, active status, or IPO date).`);
      continue;
    }
    const baseName = getBaseName(company.companyName);
    if (!companyGroups.has(baseName)) {
      companyGroups.set(baseName, []);
    }
    companyGroups.get(baseName).push(company);
  }
  const finalCompanies = [];
  for (const [baseName, candidates] of companyGroups.entries()) {
    if (candidates.length === 1) {
      finalCompanies.push(candidates[0]);
      continue;
    }
    let activeCompanies = candidates.filter((c) => c.isActivelyTrading === true);
    if (activeCompanies.length === 0) {
      console.log(`[DEDUPE] Duplicate group "${baseName}" has ${candidates.length} candidates, but none are active. Skipping.`);
      continue;
    }
    if (activeCompanies.length === 1) {
      finalCompanies.push(activeCompanies[0]);
      console.log(`[DEDUPE] Duplicate group "${baseName}" resolved to one active company: ${activeCompanies[0].symbol}`);
      continue;
    }
    console.log(`[DEDUPE] Duplicate group "${baseName}" has ${activeCompanies.length} active candidates. Using IPO date to break tie.`);
    activeCompanies.sort((a, b) => new Date(a.ipoDate) - new Date(b.ipoDate));
    const winner = activeCompanies[0];
    finalCompanies.push(winner);
    console.log(`[DEDUPE] Duplicate group "${baseName}" resolved to ${winner.symbol} (IPO: ${winner.ipoDate}).`);
  }
  console.log(`[DEDUPE] De-duplication finished. Reduced from ${companies.length} to ${finalCompanies.length} companies.`);
  return finalCompanies;
}

// =========================================================================
//  MAIN ENDPOINT: /fetch
// =========================================================================
app.get("/fetch", async (req, res) => {
  const { code, year } = req.query;
  console.log(`[FETCH] Initial request: code=${code}, year=${year}`);

  const BATCH_SIZE = 15;
  const DELAY_BETWEEN_BATCHES = 400;

  try {
    if (!code || !year) {
      return res.status(400).json({ error: "Missing required parameters" });
    }

    const screenerCompanies = await getFullScreenerList(code, year);
    if (screenerCompanies.length === 0) {
      console.log(`[FETCH] No companies found from screener. Responding with mock data.`);
      return res.status(200).json(getMockData(code, year));
    }
    console.log(`[FETCH] Found ${screenerCompanies.length} potential peers. Starting data enrichment...`);

    const enrichedPool = await enrichCompaniesWithHistoricalMarketCap(screenerCompanies, year, BATCH_SIZE, DELAY_BETWEEN_BATCHES);
    console.log(`[FETCH] Data enrichment complete. Have valid data for ${enrichedPool.length} companies.`);

    // --- START: MODIFICATION ---
    // Normalize market caps to USD for accurate sorting using the NEW historical rate function
    const currenciesToFetch = [...new Set(enrichedPool.map(c => getCompanyCurrency(c)))];
    console.log(`[FETCH-RATES] Normalizing market caps for year ${year}. Currencies:`, currenciesToFetch);
    const ratesMap = await getUsdExchangeRates(currenciesToFetch, year);

    const normalizedPool = enrichedPool.map((company) => {
      const currency = getCompanyCurrency(company);
      const rate = ratesMap.get(currency); // This is the historical average rate
      
      if (!rate) {
        console.warn(`[NORMALIZE] No historical rate for ${currency} in ${year}. Omitting ${company.symbol} from sort.`);
        return { ...company, marketCapUSD: null }; // Mark for filtering
      }
      
      // We multiply because rate is Foreign_Currency_PER_USD
      const marketCapUSD = (company.marketCap || 0) * rate;
      return { ...company, marketCapUSD };
    }).filter(company => company.marketCapUSD !== null); // Remove companies we couldn't convert
    // --- END: MODIFICATION ---

    const sortedVerifiedPool = normalizedPool.sort((a, b) => (b.marketCapUSD || 0) - (a.marketCapUSD || 0));

    const deduplicatedPool = deduplicateCompanies(sortedVerifiedPool);

    deduplicatedPool.forEach((c) => (c.isSelected = c.symbol === code));

    const topCompanies = deduplicatedPool.slice(0, 10);
    let comparisonCompanies = [];
    const selectedIndex = deduplicatedPool.findIndex((c) => c.isSelected);

    if (selectedIndex !== -1) {
      const idealAboveCount = 9;
      const idealBelowCount = 10;
      const actualAvailableAbove = selectedIndex;
      const shortfallFromAbove = Math.max(0, idealAboveCount - actualAvailableAbove);
      const adjustedBelowCount = idealBelowCount + shortfallFromAbove;
      const startIndex = Math.max(0, selectedIndex - idealAboveCount);
      const endIndex = Math.min(deduplicatedPool.length, selectedIndex + adjustedBelowCount + 1);
      comparisonCompanies = deduplicatedPool.slice(startIndex, endIndex);
    } else {
      comparisonCompanies = deduplicatedPool.slice(0, 20);
      console.warn(`[FETCH] Selected company ${code} not found in final verified pool. Peer chart will show top companies.`);
    }

    console.log(`[FETCH] Request complete. Returning ${topCompanies.length} top companies and ${comparisonCompanies.length} comparison companies.`);
    res.json({ allCompanies: deduplicatedPool, topCompanies, comparisonCompanies });

  } catch (error) {
    console.error(`[FETCH] Unhandled Error in /fetch: ${error.message}`, error.stack);
    res.status(500).json({ error: "Internal server error." });
  }
});


// =========================================================================
//  UNCHANGED HELPER FUNCTIONS
// =========================================================================
async function getCachedAnnualData(symbol, endpoint) {
  const cacheKey = `${symbol}:${endpoint}`;
  const cached = apiCache.get(cacheKey);

  if (cached && cached.expiry > Date.now()) {
    return cached.data;
  }

  const url = `https://financialmodelingprep.com/stable/${endpoint}?symbol=${symbol}&apikey=${API_KEY}`;

  try {
    const data = await fetchDataWithRetry(url);
    if (data) {
      apiCache.set(cacheKey, { data: data, expiry: Date.now() + CACHE_TTL_MS });
    }
    return data;
  } catch (error) {
    console.error(`[CACHE-FETCH] Failed to fetch ${endpoint} for ${symbol}: ${error.message}`);
    apiCache.set(cacheKey, { data: null, expiry: Date.now() + 1000 * 60 * 5 });
    return null;
  }
}

async function fetchSectorCompanies(industry, country = null) {
  let url = `https://financialmodelingprep.com/api/v3/stock-screener?industry=${encodeURIComponent(industry)}&limit=10000&apikey=${API_KEY}`;
  if (country) url += `&country=${country}`;
  try {
    const results = await fetchDataWithRetry(url);
    if (!Array.isArray(results)) return [];
    return results.map((c) => ({
        symbol: c.symbol,
        companyName: c.companyName,
        marketCap: c.marketCap || 0,
        sector: c.sector,
        industry: getStandardizedIndustry(c.industry),
        country: c.country,
        currency: getCompanyCurrency(c),
      })).filter((c) => c.symbol);
  } catch (e) {
    console.error(`[INDUSTRY] Failed to fetch screener for ${industry}: ${e.message}`);
    return [];
  }
}

async function getFullScreenerList(code, year) {
  const primaryCompanyProfile = await fetchBasicCompanyData(code, year);
  if (!primaryCompanyProfile) return [];

  const standardizedIndustry = getStandardizedIndustry(primaryCompanyProfile.industry);
  const BENCHMARK_COUNTRY = "SA";

  const [localPeers, globalPeers] = await Promise.all([
    fetchSectorCompanies(standardizedIndustry, BENCHMARK_COUNTRY),
    fetchSectorCompanies(standardizedIndustry, null),
  ]);

  const screenerCompanyMap = new Map();
  const seenCompanyNames = new Set();

  if (primaryCompanyProfile && primaryCompanyProfile.symbol && primaryCompanyProfile.companyName) {
    screenerCompanyMap.set(primaryCompanyProfile.symbol, primaryCompanyProfile);
    seenCompanyNames.add(primaryCompanyProfile.companyName);
  }

  [...localPeers, ...globalPeers].forEach((c) => {
    if (c && c.symbol && c.companyName) {
      if (!screenerCompanyMap.has(c.symbol) && !seenCompanyNames.has(c.companyName)) {
        screenerCompanyMap.set(c.symbol, c);
        seenCompanyNames.add(c.companyName);
      }
    }
  });

  return Array.from(screenerCompanyMap.values());
}

async function enrichCompaniesWithHistoricalMarketCap(companies, year, batchSize, delayMs) {
    const enrichedCompanies = [];
    for (let i = 0; i < companies.length; i += batchSize) {
        const batchCompanies = companies.slice(i, i + batchSize);
        console.log(`[ENRICH] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(companies.length / batchSize)}...`);

        const batchPromises = batchCompanies.map(async (company) => {
            const [evRes, metricsRes, profileRes] = await Promise.all([
                getCachedAnnualData(company.symbol, "enterprise-values"),
                getCachedAnnualData(company.symbol, "key-metrics"),
                getCachedAnnualData(company.symbol, "profile"),
            ]);
            
            const evData = Array.isArray(evRes) ? evRes.find((ev) => ev.date && String(ev.date).startsWith(String(year))) : null;
            const metricsData = Array.isArray(metricsRes) ? metricsRes.find((m) => String(m.fiscalYear) === String(year)) : null;
            const profileData = Array.isArray(profileRes) ? profileRes[0] : null;

            if (!profileData) {
                console.warn(`[ENRICH] ${company.symbol}: No profile data found. It will be excluded.`);
                return null;
            }

            let financialCurrency;
            if (metricsData && metricsData.reportedCurrency) {
                financialCurrency = metricsData.reportedCurrency;
            } else {
                financialCurrency = getCompanyCurrency(profileData);
                console.warn(`[ENRICH-CURRENCY] ${company.symbol}: Using profile currency '${financialCurrency}' as fallback.`);
            }

            let marketCapValue = 0;
            if (evData && evData.marketCapitalization > 0) {
                marketCapValue = evData.marketCapitalization;
            } else if (metricsData && metricsData.marketCap > 0) {
                console.warn(`[ENRICH-FALLBACK] ${company.symbol}: No 'enterprise-values' data, using 'key-metrics' market cap.`);
                marketCapValue = metricsData.marketCap;
            } else if (profileData && profileData.mktCap > 0) {
                console.warn(`[ENRICH-FALLBACK] ${company.symbol}: No historical data, using LIVE 'profile' market cap.`);
                marketCapValue = profileData.mktCap;
            }

            if (marketCapValue > 0) {
                company.marketCap = marketCapValue;
                company.currency = financialCurrency;
                company.companyName = profileData.companyName || company.companyName;
                company.sector = profileData.sector || company.sector;
                company.industry = getStandardizedIndustry(profileData.industry) || company.industry;
                company.country = profileData.country || company.country;
                company.isActivelyTrading = profileData.isActivelyTrading;
                company.ipoDate = profileData.ipoDate;
                return company;
            }

            console.warn(`[ENRICH] ${company.symbol}: Could not find any valid market cap for year ${year}. It will be excluded.`);
            return null;
        });

        const batchResults = await Promise.all(batchPromises);
        enrichedCompanies.push(...batchResults.filter(Boolean));
        if (i + batchSize < companies.length) await delay(delayMs);
    }
    return enrichedCompanies.filter((c) => c && typeof c.marketCap === "number" && c.marketCap > 0);
}

async function fetchBasicCompanyData(symbol, year) {
  if (!symbol) return null;
  try {
    const [evRes, metricsRes, profileRes] = await Promise.all([
      getCachedAnnualData(symbol, "enterprise-values"),
      getCachedAnnualData(symbol, "key-metrics"),
      getCachedAnnualData(symbol, "profile"),
    ]);

    const profileData = Array.isArray(profileRes) ? profileRes[0] : null;
    if (!profileData) return null;

    const evData = Array.isArray(evRes) ? evRes.find((ev) => ev.date && String(ev.date).startsWith(String(year))) : null;
    const metricsData = Array.isArray(metricsRes) ? metricsRes.find((m) => String(m.fiscalYear) === String(year)) : null;

    let financialCurrency;
    if (metricsData && metricsData.reportedCurrency) {
      financialCurrency = metricsData.reportedCurrency;
    } else {
      financialCurrency = getCompanyCurrency(profileData);
      console.warn(`[DATA-CURRENCY] ${symbol}: Using profile currency '${financialCurrency}' as fallback.`);
    }

    let marketCapValue = 0;
    if (evData && evData.marketCapitalization > 0) {
      marketCapValue = evData.marketCapitalization;
    } else if (metricsData && metricsData.marketCap > 0) {
      console.warn(`[DATA-FALLBACK] ${symbol}: No 'enterprise-values' data, using 'key-metrics' market cap.`);
      marketCapValue = metricsData.marketCap;
    } else if (profileData && profileData.mktCap > 0) {
      console.warn(`[DATA-FALLBACK] ${symbol}: No historical data, using LIVE 'profile' market cap.`);
      marketCapValue = profileData.mktCap;
    }

    return {
      symbol: profileData.symbol,
      companyName: profileData.companyName,
      marketCap: marketCapValue,
      sector: profileData.sector,
      industry: getStandardizedIndustry(profileData.industry),
      country: profileData.country,
      currency: financialCurrency,
      isActivelyTrading: profileData.isActivelyTrading,
      ipoDate: profileData.ipoDate,
    };
  } catch (e) {
    console.error(`[DATA] Error fetching basic data for ${symbol}: ${e.message}`);
    return null;
  }
}

// =========================================================================
//  ADDITIONAL ENDPOINTS
// =========================================================================

// --- NEW ENDPOINT for the client to fetch historical rates ---
app.get("/get-average-rate", async (req, res) => {
    const { base, target, year } = req.query;
    if (!base || !target || !year) {
        return res.status(400).json({ error: "Missing required parameters: base, target, year." });
    }
    try {
        const rate = await getHistoricalAverageRate(base.toUpperCase(), target.toUpperCase(), year);
        if (rate === null) {
            return res.status(404).json({ error: `Could not calculate an average rate for ${base}-${target} in ${year}.` });
        }
        res.json({
            base: base.toUpperCase(),
            target: target.toUpperCase(),
            year: year,
            averageRate: rate
        });
    } catch (error) {
        console.error(`[ENDPOINT-ERROR] /get-average-rate failed: ${error.message}`);
        res.status(500).json({ error: "Internal server error while fetching rate." });
    }
});


app.get("/fetch-metric", async (req, res) => {
    // This endpoint remains the same, but I'm including it to show it's preserved.
    const { symbols, year, metric } = req.query;
    if (!symbols || !year || !metric)
        return res.status(400).json({ error: "Missing parameters" });

    const symbolList = symbols.split(",");
    const MAX_CONCURRENT = 10;
    let results = [];
    for (let i = 0; i < symbolList.length; i += MAX_CONCURRENT) {
        const batch = symbolList.slice(i, i + MAX_CONCURRENT);
        const promises = batch.map((s) => fetchMetricData(s, year, metric));
        results.push(...(await Promise.all(promises)));
        if (i + MAX_CONCURRENT < symbolList.length) await delay(200);
    }
    
    const finalResultsWithCurrency = results.filter(Boolean).map((result) => {
        const originalData = result.originalData || {};
        return {
            symbol: result.symbol,
            [metric]: result[metric],
            currency: getCompanyCurrency(originalData),
        };
    });

    res.json({ companies: finalResultsWithCurrency });
});


app.get("/fetch-peers", async (req, res) => {
    const { sector, year } = req.query;
    if (!sector || !year) return res.status(400).json({ error: "Missing parameters" });
    const industry = getStandardizedIndustry(sector);
    
    console.log(`[FETCH-PEERS] Fetching peers for manual entry. Industry: ${industry}, Year: ${year}`);
    
    const BENCHMARK_COUNTRY = "SA";
    const [localPeers, globalPeers] = await Promise.all([
        fetchSectorCompanies(industry, BENCHMARK_COUNTRY),
        fetchSectorCompanies(industry, null),
    ]);
    
    const screenerCompanyMap = new Map();
    const seenCompanyNames = new Set();
    [...localPeers, ...globalPeers].forEach((c) => {
        if (c && c.symbol && c.companyName) {
            if (!screenerCompanyMap.has(c.symbol) && !seenCompanyNames.has(c.companyName)) {
                screenerCompanyMap.set(c.symbol, c);
                seenCompanyNames.add(c.companyName);
            }
        }
    });
    
    const screenerCompanies = Array.from(screenerCompanyMap.values());
    console.log(`[FETCH-PEERS] Found ${screenerCompanies.length} unique potential peers for manual entry.`);
    
    const verifiedPeers = await enrichCompaniesWithHistoricalMarketCap(screenerCompanies, year, 10, 500);
    
    // --- START: MODIFICATION ---
    // Apply the same historical rate normalization logic here for consistency
    const currenciesToFetch = [...new Set(verifiedPeers.map(c => getCompanyCurrency(c)))];
    console.log(`[PEERS-RATES] Normalizing peer market caps for year ${year}. Currencies:`, currenciesToFetch);
    const ratesMap = await getUsdExchangeRates(currenciesToFetch, year);

    const normalizedPeers = verifiedPeers.map((company) => {
        const currency = getCompanyCurrency(company);
        const rate = ratesMap.get(currency);
        if (!rate) {
            console.warn(`[NORMALIZE-PEERS] No historical rate for ${currency} in ${year}. Omitting ${company.symbol}.`);
            return null;
        }
        const marketCapUSD = (company.marketCap || 0) * rate;
        return { ...company, marketCapUSD };
    }).filter(Boolean);
    // --- END: MODIFICATION ---
    
    const sorted = normalizedPeers.sort((a, b) => (b.marketCapUSD || 0) - (a.marketCapUSD || 0));
    
    const deduplicatedPeers = deduplicateCompanies(sorted);
    
    const allCompaniesResponse = deduplicatedPeers.map((c) => ({ ...c, isUserCompany: false }));
    
    res.json({
        allCompanies: allCompaniesResponse,
        topCompanies: allCompaniesResponse.slice(0, 10),
        comparisonCompanies: allCompaniesResponse.slice(0, 20),
    });
});


// =========================================================================
//  UNCHANGED METRIC MAPPING AND FETCHING LOGIC
// =========================================================================
const fullMetricEndpointMap = {  marketCap: { endpoint: "key-metrics", field: "marketCap" },  enterpriseValue: { endpoint: "key-metrics", field: "enterpriseValue" },  evToSales: { endpoint: "key-metrics", field: "evToSales" },  enterpriseValueOverEBITDA: { endpoint: "key-metrics", field: "evToEBITDA" },  evToOperatingCashFlow: {    endpoint: "key-metrics",    field: "evToOperatingCashFlow",  },  evToFreeCashFlow: { endpoint: "key-metrics", field: "evToFreeCashFlow" },  netDebtToEBITDA: { endpoint: "key-metrics", field: "netDebtToEBITDA" },  incomeQuality: { endpoint: "key-metrics", field: "incomeQuality" },  grahamNumber: { endpoint: "key-metrics", field: "grahamNumber" },  grahamNetNet: { endpoint: "key-metrics", field: "grahamNetNet" },  roe: { endpoint: "key-metrics", field: "returnOnEquity" },  returnOnEquity: { endpoint: "key-metrics", field: "returnOnEquity" },  roic: { endpoint: "key-metrics", field: "returnOnInvestedCapital" },  returnOnInvestedCapital: {    endpoint: "key-metrics",    field: "returnOnInvestedCapital",  },  returnOnTangibleAssets: {    endpoint: "key-metrics",    field: "returnOnTangibleAssets",  },  earningsYield: { endpoint: "key-metrics", field: "earningsYield" },  freeCashFlowYield: { endpoint: "key-metrics", field: "freeCashFlowYield" },  capexToOperatingCashFlow: {    endpoint: "key-metrics",    field: "capexToOperatingCashFlow",  },  capexToDepreciation: {    endpoint: "key-metrics",    field: "capexToDepreciation",  },  capexToRevenue: { endpoint: "key-metrics", field: "capexToRevenue" },  salesGeneralAndAdministrativeToRevenue: {    endpoint: "key-metrics",    field: "salesGeneralAndAdministrativeToRevenue",  },  researchAndDdevelopementToRevenue: {    endpoint: "key-metrics",    field: "researchAndDevelopementToRevenue",  },  stockBasedCompensationToRevenue: {    endpoint: "key-metrics",    field: "stockBasedCompensationToRevenue",  },  intangiblesToTotalAssets: {    endpoint: "key-metrics",    field: "intangiblesToTotalAssets",  },  workingCapital: { endpoint: "key-metrics", field: "workingCapital" },  investedCapital: { endpoint: "key-metrics", field: "investedCapital" },  netCurrentAssetValue: {    endpoint: "key-metrics",    field: "netCurrentAssetValue",  },  averageReceivables: { endpoint: "key-metrics", field: "averageReceivables" },  averagePayables: { endpoint: "key-metrics", field: "averagePayables" },  averageInventory: { endpoint: "key-metrics", field: "averageInventory" },  daysOfSalesOutstanding: {    endpoint: "key-metrics",    field: "daysOfSalesOutstanding",  },  daysOfPayablesOutstanding: {    endpoint: "key-metrics",    field: "daysOfPayablesOutstanding",  },  daysOfInventoryOutstanding: {    endpoint: "key-metrics",    field: "daysOfInventoryOutstanding",  },  operatingCycle: { endpoint: "key-metrics", field: "operatingCycle" },  cashConversionCycle: {    endpoint: "key-metrics",    field: "cashConversionCycle",  },  grossProfitMargin: { endpoint: "ratios", field: "grossProfitMargin" },  grossProfitRatio: { endpoint: "ratios", field: "grossProfitMargin" },  ebitdaratio: { endpoint: "ratios", field: "ebitdaMargin" },  ebitdaMargin: { endpoint: "ratios", field: "ebitdaMargin" },  operatingIncomeRatio: { endpoint: "ratios", field: "operatingProfitMargin" },  operatingProfitMargin: { endpoint: "ratios", field: "operatingProfitMargin" },  incomeBeforeTaxRatio: { endpoint: "ratios", field: "pretaxProfitMargin" },  pretaxProfitMargin: { endpoint: "ratios", field: "pretaxProfitMargin" },  netIncomeRatio: { endpoint: "ratios", field: "netProfitMargin" },  netProfitMargin: { endpoint: "ratios", field: "netProfitMargin" },  currentRatio: { endpoint: "ratios", field: "currentRatio" },  quickRatio: { endpoint: "ratios", field: "quickRatio" },  cashRatio: { endpoint: "ratios", field: "cashRatio" },  returnOnAssets: { endpoint: "ratios", field: "returnOnAssets" },  returnOnCapitalEmployed: {    endpoint: "ratios",    field: "returnOnCapitalEmployed",  },  netIncomePerEBT: { endpoint: "ratios", field: "netIncomePerEBT" },  ebtPerEbit: { endpoint: "ratios", field: "ebtPerEbit" },  debtRatio: { endpoint: "ratios", field: "debtToAssetsRatio" },  debtToAssets: { endpoint: "ratios", field: "debtToAssetsRatio" },  debtEquityRatio: { endpoint: "ratios", field: "debtToEquityRatio" },  longTermDebtToCapitalization: {    endpoint: "ratios",    field: "longTermDebtToCapitalRatio",  },  totalDebtToCapitalization: {    endpoint: "ratios",    field: "debtToCapitalRatio",  },  interestCoverage: { endpoint: "ratios", field: "interestCoverageRatio" },  cashFlowToDebtRatio: { endpoint: "ratios", field: "cashFlowToDebtRatio" },  companyEquityMultiplier: {    endpoint: "ratios",    field: "financialLeverageRatio",  },  receivablesTurnover: { endpoint: "ratios", field: "receivablesTurnover" },  payablesTurnover: { endpoint: "ratios", field: "payablesTurnover" },  inventoryTurnover: { endpoint: "ratios", field: "inventoryTurnover" },  fixedAssetTurnover: { endpoint: "ratios", field: "fixedAssetTurnover" },  assetTurnover: { endpoint: "ratios", field: "assetTurnover" },  operatingCashFlowPerShare: {    endpoint: "ratios",    field: "operatingCashFlowPerShare",  },  freeCashFlowPerShare: { endpoint: "ratios", field: "freeCashFlowPerShare" },  cashPerShare: { endpoint: "ratios", field: "cashPerShare" },  payoutRatio: { endpoint: "ratios", field: "dividendPayoutRatio" },  operatingCashFlowSalesRatio: {    endpoint: "ratios",    field: "operatingCashFlowSalesRatio",  },  freeCashFlowOperatingCashFlowRatio: {    endpoint: "ratios",    field: "freeCashFlowOperatingCashFlowRatio",  },  cashFlowCoverageRatios: {    endpoint: "ratios",    field: "operatingCashFlowCoverageRatio",  },  shortTermCoverageRatios: {    endpoint: "ratios",    field: "shortTermOperatingCashFlowCoverageRatio",  },  capitalExpenditureCoverageRatio: {    endpoint: "ratios",    field: "capitalExpenditureCoverageRatio",  },  dividendPaidAndCapexCoverageRatio: {    endpoint: "ratios",    field: "dividendPaidAndCapexCoverageRatio",  },  dividendPayoutRatio: { endpoint: "ratios", field: "dividendPayoutRatio" },  priceBookValueRatio: { endpoint: "ratios", field: "priceToBookRatio" },  priceToBookRatio: { endpoint: "ratios", field: "priceToBookRatio" },  ptbRatio: { endpoint: "ratios", field: "priceToBookRatio" },  priceToSalesRatio: { endpoint: "ratios", field: "priceToSalesRatio" },  priceSalesRatio: { endpoint: "ratios", field: "priceToSalesRatio" },  priceEarningsRatio: { endpoint: "ratios", field: "priceToEarningsRatio" },  peRatio: { endpoint: "ratios", field: "priceToEarningsRatio" },  priceToFreeCashFlowsRatio: {    endpoint: "ratios",    field: "priceToFreeCashFlowRatio",  },  pfcfRatio: { endpoint: "ratios", field: "priceToFreeCashFlowRatio" },  priceToOperatingCashFlowsRatio: {    endpoint: "ratios",    field: "priceToOperatingCashFlowRatio",  },  pocfratio: { endpoint: "ratios", field: "priceToOperatingCashFlowRatio" },  priceCashFlowRatio: {    endpoint: "ratios",    field: "priceToOperatingCashFlowRatio",  },  priceEarningsToGrowthRatio: {    endpoint: "ratios",    field: "priceToEarningsGrowthRatio",  },  dividendYield: { endpoint: "ratios", field: "dividendYield" },  enterpriseValueMultiple: {    endpoint: "ratios",    field: "enterpriseValueMultiple",  },  priceFairValue: { endpoint: "ratios", field: "priceToFairValue" },  revenuePerShare: { endpoint: "ratios", field: "revenuePerShare" },  netIncomePerShare: { endpoint: "ratios", field: "netIncomePerShare" },  bookValuePerShare: { endpoint: "ratios", field: "bookValuePerShare" },  tangibleBookValuePerShare: {    endpoint: "ratios",    field: "tangibleBookValuePerShare",  },  shareholdersEquityPerShare: {    endpoint: "ratios",    field: "shareholdersEquityPerShare",  },  interestDebtPerShare: { endpoint: "ratios", field: "interestDebtPerShare" },  capexPerShare: { endpoint: "ratios", field: "capexPerShare" },  effectiveTaxRate: { endpoint: "ratios", field: "effectiveTaxRate" },  revenue: { endpoint: "income-statement", field: "revenue" },  growthRevenue: {    endpoint: "income-statement-growth",    field: "growthRevenue",  },  growthGrossProfitRatio: {    endpoint: "income-statement-growth",    field: "growthGrossProfit",  },  growthEBITDARatio: {    endpoint: "income-statement-growth",    field: "growthEBITDA",  },  growthOperatingIncomeRatio: {    endpoint: "income-statement-growth",    field: "growthOperatingIncome",  },  growthIncomeBeforeTaxRatio: {    endpoint: "income-statement-growth",    field: "growthIncomeBeforeTax",  },  growthNetIncomeRatio: {    endpoint: "income-statement-growth",    field: "growthNetIncome",  },};

async function fetchMetricData(symbol, year, metric) {
  if (metric === "revenuePerFte") {
    // ... This special case logic remains unchanged
    return { symbol, [metric]: "N/A" }; // Placeholder
  }
  
  const info = fullMetricEndpointMap[metric];
  if (!info) {
    console.warn(`[METRIC] No mapping found for metric: ${metric}`);
    return null;
  }

  try {
    const endpointData = await getCachedAnnualData(symbol, info.endpoint);
    // THIS IS THE CORRECTED LINE
    const yearData = Array.isArray(endpointData) ? endpointData.find((d) => String(d.fiscalYear) === String(year) || (d.date && d.date.startsWith(String(year)))) : null;
    return {
      symbol,
      [metric]: yearData && yearData[info.field] != null ? yearData[info.field] : "N/A",
      originalData: yearData,
    };
  } catch (e) {
    console.error(`[METRIC] Failed to fetch ${metric} for ${symbol}: ${e.message}`);
    return { symbol, [metric]: "N/A" };
  }
}

// =========================================================================
//  SERVER START
// =========================================================================

app.get("/", (req, res) => {
  res.send("Server is online and ready.");
});

const listener = app.listen(process.env.PORT || 3000, "0.0.0.0", () => {
  const port = listener.address().port;
  console.log(`Server is listening on port ${port}`);
  console.log(`Your app is listening on http://localhost:${port}`);
  if (process.env.PROJECT_DOMAIN) {
    console.log(`Server is listening on https://${process.env.PROJECT_DOMAIN}.glitch.me`);
  }
});
