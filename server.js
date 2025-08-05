const express = require("express");
const https = require("https");
const cors = require("cors");
require("dotenv").config();

const app = express();
const API_KEY = process.env.API_KEY;

// --- OPTIMIZATION 1: Centralized In-Memory Cache ---
const apiCache = new Map();
const CACHE_TTL_MS = 1000 * 60 * 30; // Cache data for 30 minutes

// --- START: MODIFICATION ---
// New cache for yearly average exchange rates. Key: year, Value: rates object
const yearlyAverageRateCache = new Map();

/**
 * Fetches and calculates the average exchange rates for a specific year using the exchangerate.host API.
 * It samples the rate from the 15th of each month to approximate the yearly average.
 * The results are cached indefinitely.
 * @param {string|number} year The year for which to calculate average rates.
 * @returns {Promise<object>} An object containing the average rates relative to USD.
 */
async function getYearlyAverageUsdExchangeRates(year) {
    if (yearlyAverageRateCache.has(year)) {
        console.log(`[RATES-CACHE] Using cached average exchange rates for ${year}.`);
        return yearlyAverageRateCache.get(year);
    }

    console.log(`[RATES-AVG] Calculating average exchange rates for ${year} using exchangerate.host...`);

    // Define the currencies you want to support (as uppercase symbols).
    const currenciesToAverage = ['SAR', 'EUR', 'GBP', 'CAD', 'JPY', 'AED', 'KWD'];

    const monthlyRatePromises = [];

    // Create API calls for the 15th of each month.
    for (let month = 1; month <= 12; month++) {
        const dateString = `${year}-${String(month).padStart(2, '0')}-15`;
        const url = `https://api.exchangerate.host/${dateString}?base=USD&symbols=${currenciesToAverage.join(',')}`;
        monthlyRatePromises.push(fetchDataWithRetry(url).catch(e => {
            console.warn(`[RATES-AVG] Could not fetch rates for date ${dateString}: ${e.message}`);
            return null;
        }));
    }

    const monthlyResults = await Promise.all(monthlyRatePromises);
    const validMonthlyResults = monthlyResults.filter(r => r && r.success);

    if (validMonthlyResults.length < 6) { // If less than half the year's data is available, fail.
        console.error(`[RATES-AVG] Failed to fetch sufficient historical rate data for ${year}.`);
        // Return a default object to prevent crashes, but log the error.
        return { usd: 1.0 }; 
    }

    const averageRates = { usd: 1.0 };

    for (const currency of currenciesToAverage) {
        const validRates = validMonthlyResults
            .map(result => result && result.rates ? result.rates[currency.toUpperCase()] : null)
            .filter(rate => typeof rate === 'number');

        if (validRates.length > 0) {
            const sum = validRates.reduce((acc, rate) => acc + rate, 0);
            averageRates[currency.toLowerCase()] = sum / validRates.length;
        } else {
            console.warn(`[RATES-AVG] Could not calculate average for ${currency} for ${year}.`);
            averageRates[currency.toLowerCase()] = null;
        }
    }

    yearlyAverageRateCache.set(year, averageRates);
    console.log(`[RATES-AVG] Finished calculating averages for ${year}.`, averageRates);
    return averageRates;
}
// --- END: MODIFICATION ---

// --- CORE HELPER FUNCTIONS ---

/**
 * Determines the currency for a company, defaulting to USD.
 */
function getCompanyCurrency(company) {
  // First, check the 'reportedCurrency' field from the new endpoints
  if (company && company.reportedCurrency)
    return company.reportedCurrency.toUpperCase();
  if (company && company.currency) return company.currency.toUpperCase();
  if (company && company.symbol) {
    const symbol = company.symbol.toUpperCase();
    if (symbol.endsWith(".SR")) return "SAR";
    // Add other suffixes as needed (.L, .TO, etc.)
  }
  return "USD";
}

/**
 * Standardizes industry names to handle API inconsistencies.
 */
function getStandardizedIndustry(industry) {
  if (!industry) return "Unknown";
  const normalized = industry.toLowerCase().trim();
  if (normalized.includes("telecom")) {
    return "Telecommunications Services";
  }
  // Add other rules as needed
  return industry;
}

/**
 * A simple promise-based delay function.
 */
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Returns mock data if the API fails catastrophically.
 */
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

// --- EXPRESS APP SETUP ---

app.use(
  cors({
    origin: "*",
    methods: ["GET", "POST", "OPTIONS"],
    allowedHeaders: ["Content-Type", "Authorization"],
    credentials: false,
  }),
);

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET", "POST", "OPTIONS");
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type",
    "Authorization",
  );
  if (req.method === "OPTIONS") {
    return res.status(200).end();
  }
  next();
});

app.use(express.static(__dirname));

app.use((req, res, next) => {
  console.log(`[REQUEST] ${req.method} ${req.url}`);
  next();
});

// --- START: NEW DUPLICATE HANDLING ---
/**
 * De-duplicates a list of companies based on name similarity and specific rules.
 * Rules:
 * 1. Groups companies by a "base name" (e.g., "AT&T Inc 5.35%" and "AT&T Inc" both become "AT&T Inc").
 * 2. If a group has duplicates, it prefers companies where `isActivelyTrading` is true.
 * 3. If multiple companies are actively trading, it chooses the one with the oldest `ipoDate`.
 * 4. If a group of duplicates has no actively trading companies, it is removed entirely.
 * 5. Unique companies (not duplicates) are ALWAYS kept.
 */
function deduplicateCompanies(companies) {
  if (!companies || companies.length === 0) {
    return [];
  }
  console.log(
    `[DEDUPE] Starting de-duplication process on ${companies.length} companies.`,
  );

  const companyGroups = new Map();

  // Heuristic to find the "base name".
  const getBaseName = (name) => {
    if (!name) return "";
    return name.replace(/\s+\d+(\.\d+)?%.*$/, "").trim();
  };

  // Step 1: Group companies by their base name.
  for (const company of companies) {
    if (
      !company.companyName ||
      company.isActivelyTrading === undefined ||
      !company.ipoDate
    ) {
      console.warn(
        `[DEDUPE] Skipping company ${company.symbol} because it's missing required fields (name, active status, or IPO date).`,
      );
      continue;
    }

    const baseName = getBaseName(company.companyName);
    if (!companyGroups.has(baseName)) {
      companyGroups.set(baseName, []);
    }
    companyGroups.get(baseName).push(company);
  }

  const finalCompanies = [];
  // Step 2: Process each group to find the single best candidate.
  for (const [baseName, candidates] of companyGroups.entries()) {
    // **CORRECTED LOGIC**: If a company is unique by name, it's not a duplicate. Keep it.
    if (candidates.length === 1) {
      finalCompanies.push(candidates[0]);
      continue;
    }

    // --- The following logic now ONLY applies to actual duplicates (groups with >1 company) ---

    // Rule 1: Filter for actively trading companies.
    let activeCompanies = candidates.filter(
      (c) => c.isActivelyTrading === true,
    );

    if (activeCompanies.length === 0) {
      // All candidates in the duplicate group are inactive. As per the logic, we don't include any of them.
      console.log(
        `[DEDUPE] Duplicate group "${baseName}" has ${candidates.length} candidates, but none are active. Skipping.`,
      );
      continue;
    }

    if (activeCompanies.length === 1) {
      // Exactly one is active. This is our winner for the duplicate group.
      finalCompanies.push(activeCompanies[0]);
      console.log(
        `[DEDUPE] Duplicate group "${baseName}" resolved to one active company: ${activeCompanies[0].symbol}`,
      );
      continue;
    }

    // Rule 2: Tie-breaker. More than one is active, so we use the oldest IPO date.
    console.log(
      `[DEDUPE] Duplicate group "${baseName}" has ${activeCompanies.length} active candidates. Using IPO date to break tie.`,
    );
    activeCompanies.sort((a, b) => new Date(a.ipoDate) - new Date(b.ipoDate)); // Sorts by date, oldest first

    const winner = activeCompanies[0];
    finalCompanies.push(winner);
    console.log(
      `[DEDUPE] Duplicate group "${baseName}" resolved to ${winner.symbol} (IPO: ${winner.ipoDate}).`,
    );
  }

  console.log(
    `[DEDUPE] De-duplication finished. Reduced from ${companies.length} to ${finalCompanies.length} companies.`,
  );
  return finalCompanies;
}
// --- END: NEW DUPLICATE HANDLING ---

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
      return res.status(400).json({
        error: "Missing required parameters",
      });
    }

    // --- Step 1: Get the full list of potential peers from the screener ---
    const screenerCompanies = await getFullScreenerList(code, year);
    if (screenerCompanies.length === 0) {
      console.log(
        `[FETCH] No companies found from screener. Responding with mock data.`,
      );
      return res.status(200).json(getMockData(code, year));
    }
    console.log(
      `[FETCH] Found ${screenerCompanies.length} potential peers. Starting data enrichment...`,
    );

    // --- Step 2: Efficient Data Enrichment ---
    const enrichedPool = await enrichCompaniesWithHistoricalMarketCap(
      screenerCompanies,
      year,
      BATCH_SIZE,
      DELAY_BETWEEN_BATCHES,
    );
    console.log(
      `[FETCH] Data enrichment complete. Have valid data for ${enrichedPool.length} companies.`,
    );

    // --- Step 2.5: Normalize market caps to USD for accurate sorting ---
    const rates = await getYearlyAverageUsdExchangeRates(year);
    const normalizedPool = enrichedPool.map((company) => {
      const currency = (getCompanyCurrency(company) || "USD").toLowerCase();
      const rate = rates[currency];
      if (!rate) {
        console.warn(
          `[NORMALIZE] No average exchange rate found for ${currency.toUpperCase()} for year ${year}. Market cap for ${company.symbol} might be inaccurate for sorting.`,
        );
        return { ...company, marketCapUSD: company.marketCap };
      }
      const marketCapUSD = (company.marketCap || 0) / rate;
      return {
        ...company,
        marketCapUSD,
      };
    });

    // --- Step 3: Sort the enriched list using the USD-normalized market cap ---
    const sortedVerifiedPool = normalizedPool.sort(
      (a, b) => (b.marketCapUSD || 0) - (a.marketCapUSD || 0),
    );

    // --- START: NEW DUPLICATE HANDLING ---
    // Apply the de-duplication logic to the entire pool of companies
    const deduplicatedPool = deduplicateCompanies(sortedVerifiedPool);
    // --- END: NEW DUPLICATE HANDLING ---

    // --- Use the de-duplicated list for all subsequent operations ---
    deduplicatedPool.forEach((c) => (c.isSelected = c.symbol === code));

    const topCompanies = deduplicatedPool.slice(0, 10);

    let comparisonCompanies = [];
    const selectedIndex = deduplicatedPool.findIndex((c) => c.isSelected);

    if (selectedIndex !== -1) {
      const idealPeersTotal = 20;
      const idealAboveCount = 9;
      const idealBelowCount = 10;
      const actualAvailableAbove = selectedIndex;
      const shortfallFromAbove = Math.max(
        0,
        idealAboveCount - actualAvailableAbove,
      );
      const adjustedBelowCount = idealBelowCount + shortfallFromAbove;
      const startIndex = Math.max(0, selectedIndex - idealAboveCount);
      const endIndex = Math.min(
        deduplicatedPool.length,
        selectedIndex + adjustedBelowCount + 1,
      );

      comparisonCompanies = deduplicatedPool.slice(startIndex, endIndex);
    } else {
      comparisonCompanies = deduplicatedPool.slice(0, 20); // If not found, just return the top 20
      console.warn(
        `[FETCH] Selected company ${code} not found in final verified pool. Peer chart will show top companies.`,
      );
    }

    console.log(
      `[FETCH] Request complete. Returning ${topCompanies.length} top companies and ${comparisonCompanies.length} comparison companies.`,
    );
    res.json({
      allCompanies: deduplicatedPool, // Return the clean list
      topCompanies,
      comparisonCompanies,
    });
  } catch (error) {
    console.error(
      `[FETCH] Unhandled Error in /fetch: ${error.message}`,
      error.stack,
    );
    res.status(500).json({
      error: "Internal server error.",
    });
  }
});

// =========================================================================
//  DATA FETCHING & PROCESSING HELPERS
// =========================================================================

/**
 * Fetches data from the FMP API with a retry mechanism.
 */
async function fetchDataWithRetry(url, retries = 3, backoff = 500) {
  for (let i = 0; i < retries; i++) {
    try {
      return await new Promise((resolve, reject) => {
        const request = https
          .get(
            url,
            {
              timeout: 20000,
            },
            (res) => {
              if (res.statusCode < 200 || res.statusCode >= 300) {
                return reject(new Error(`HTTP Error: ${res.statusCode}`));
              }
              let body = "";
              res.on("data", (chunk) => (body += chunk));
              res.on("end", () => {
                try {
                  const parsed = JSON.parse(body);
                  if (parsed && parsed["Error Message"]) {
                    return reject(
                      new Error(`FMP API Error: ${parsed["Error Message"]}`),
                    );
                  }
                  resolve(parsed);
                } catch (e) {
                  reject(new Error("Invalid JSON response"));
                }
              });
            },
          )
          .on("error", reject)
          .on("timeout", () => {
            request.destroy();
            reject(new Error("Request timed out"));
          });
      });
    } catch (error) {
      console.warn(
        `[FETCHDATA] Attempt ${i + 1} failed for ${url.split("apikey=")[0]}: ${error.message}`,
      );
      if (i === retries - 1) throw error;
      await delay(backoff * Math.pow(2, i));
    }
  }
}

/**
 * Centralized, cached function to get annual data from the new FMP "stable" endpoints.
 */
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
      apiCache.set(cacheKey, {
        data: data,
        expiry: Date.now() + CACHE_TTL_MS,
      });
    }
    return data;
  } catch (error) {
    console.error(
      `[CACHE-FETCH] Failed to fetch ${endpoint} for ${symbol}: ${error.message}`,
    );
    apiCache.set(cacheKey, {
      data: null,
      expiry: Date.now() + 1000 * 60 * 5, // Cache failure for 5 mins
    });
    return null;
  }
}

/**
 * Fetches the list of companies from the stock screener.
 */
async function fetchSectorCompanies(industry, country = null) {
  let url = `https://financialmodelingprep.com/api/v3/stock-screener?industry=${encodeURIComponent(industry)}&limit=10000&apikey=${API_KEY}`;
  if (country) url += `&country=${country}`;
  try {
    const results = await fetchDataWithRetry(url);
    if (!Array.isArray(results)) return [];
    return results
      .map((c) => ({
        symbol: c.symbol,
        companyName: c.companyName,
        marketCap: c.marketCap || 0,
        sector: c.sector,
        industry: getStandardizedIndustry(c.industry),
        country: c.country,
        currency: getCompanyCurrency(c),
      }))
      .filter((c) => c.symbol);
  } catch (e) {
    console.error(
      `[INDUSTRY] Failed to fetch screener for ${industry}: ${e.message}`,
    );
    return [];
  }
}

/**
 * Orchestrates the initial screener fetches to get a full list of unique potential peers.
 */
async function getFullScreenerList(code, year) {
  const primaryCompanyProfile = await fetchBasicCompanyData(code, year);
  if (!primaryCompanyProfile) return [];

  const standardizedIndustry = getStandardizedIndustry(
    primaryCompanyProfile.industry,
  );
  const BENCHMARK_COUNTRY = "SA";

  const [localPeers, globalPeers] = await Promise.all([
    fetchSectorCompanies(standardizedIndustry, BENCHMARK_COUNTRY),
    fetchSectorCompanies(standardizedIndustry, null),
  ]);

  const screenerCompanyMap = new Map();
  const seenCompanyNames = new Set();

  if (
    primaryCompanyProfile &&
    primaryCompanyProfile.symbol &&
    primaryCompanyProfile.companyName
  ) {
    screenerCompanyMap.set(primaryCompanyProfile.symbol, primaryCompanyProfile);
    seenCompanyNames.add(primaryCompanyProfile.companyName);
  }

  [...localPeers, ...globalPeers].forEach((c) => {
    if (c && c.symbol && c.companyName) {
      if (
        !screenerCompanyMap.has(c.symbol) &&
        !seenCompanyNames.has(c.companyName)
      ) {
        screenerCompanyMap.set(c.symbol, c);
        seenCompanyNames.add(c.companyName);
      }
    }
  });

  return Array.from(screenerCompanyMap.values());
}

/**
 * Enriches companies with historical market cap data with robust fallbacks.
 */
async function enrichCompaniesWithHistoricalMarketCap(
  companies,
  year,
  batchSize,
  delayMs,
) {
  const enrichedCompanies = [];
  for (let i = 0; i < companies.length; i += batchSize) {
    const batchCompanies = companies.slice(i, i + batchSize);
    console.log(
      `[ENRICH] Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(companies.length / batchSize)}...`,
    );

    const batchPromises = batchCompanies.map(async (company) => {
      // Fetch all potential data sources at once for efficiency
      const [evRes, metricsRes, profileRes] = await Promise.all([
        getCachedAnnualData(company.symbol, "enterprise-values"),
        getCachedAnnualData(company.symbol, "key-metrics"),
        getCachedAnnualData(company.symbol, "profile"),
      ]);

      // Find the data for the specific year from each source
      const evData = Array.isArray(evRes)
        ? evRes.find(
            (ev) => ev.date && String(ev.date).startsWith(String(year)),
          )
        : null;
      const metricsData = Array.isArray(metricsRes)
        ? metricsRes.find((m) => String(m.fiscalYear) === String(year))
        : null;
      const profileData = Array.isArray(profileRes) ? profileRes[0] : null;

      // If we don't have basic profile info, we can't proceed.
      if (!profileData) {
        console.warn(
          `[ENRICH] ${company.symbol}: No profile data found. It will be excluded.`,
        );
        return null;
      }

      // --- START: CORRECTED LOGIC ---

      // Step 1: Reliably determine the FINANCIAL currency. Prioritize key-metrics.
      let financialCurrency;
      if (metricsData && metricsData.reportedCurrency) {
        financialCurrency = metricsData.reportedCurrency;
      } else {
        // Fallback to profile currency if key-metrics is unavailable. This is less reliable.
        financialCurrency = getCompanyCurrency(profileData);
        console.warn(
          `[ENRICH-CURRENCY] ${company.symbol}: Using profile currency '${financialCurrency}' as fallback.`,
        );
      }

      // Step 2: Determine the best market cap value, prioritizing the new endpoint.
      let marketCapValue = 0;
      if (evData && evData.marketCapitalization > 0) {
        // Primary source: /enterprise-values
        marketCapValue = evData.marketCapitalization;
      } else if (metricsData && metricsData.marketCap > 0) {
        // First fallback: /key-metrics
        console.warn(
          `[ENRICH-FALLBACK] ${company.symbol}: No 'enterprise-values' data, using 'key-metrics' market cap.`,
        );
        marketCapValue = metricsData.marketCap;
      } else if (profileData && profileData.mktCap > 0) {
        // Final fallback: live data from /profile
        console.warn(
          `[ENRICH-FALLBACK] ${company.symbol}: No historical data, using LIVE 'profile' market cap.`,
        );
        marketCapValue = profileData.mktCap;
      }

      // Step 3: If we found a valid market cap, build and return the company object.
      if (marketCapValue > 0) {
        company.marketCap = marketCapValue;
        company.currency = financialCurrency; // Use the CORRECT financial currency
        company.companyName = profileData.companyName || company.companyName;
        company.sector = profileData.sector || company.sector;
        company.industry =
          getStandardizedIndustry(profileData.industry) || company.industry;
        company.country = profileData.country || company.country;

        // --- START: NEW DUPLICATE HANDLING ---
        // Add the required fields for our de-duplication logic later.
        company.isActivelyTrading = profileData.isActivelyTrading;
        company.ipoDate = profileData.ipoDate;
        // --- END: NEW DUPLICATE HANDLING ---

        return company;
      }

      // --- END: CORRECTED LOGIC ---

      console.warn(
        `[ENRICH] ${company.symbol}: Could not find any valid market cap for year ${year}. It will be excluded.`,
      );
      return null;
    });

    const batchResults = await Promise.all(batchPromises);
    enrichedCompanies.push(...batchResults.filter(Boolean));
    if (i + batchSize < companies.length) await delay(delayMs);
  }
  return enrichedCompanies.filter(
    (c) => c && typeof c.marketCap === "number" && c.marketCap > 0,
  );
}

/**
 * Fetches the definitive annual data for a single company using the cache.
 */
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

    const evData = Array.isArray(evRes)
      ? evRes.find((ev) => ev.date && String(ev.date).startsWith(String(year)))
      : null;
    const metricsData = Array.isArray(metricsRes)
      ? metricsRes.find((m) => String(m.fiscalYear) === String(year))
      : null;

    // --- START: CORRECTED LOGIC ---

    // Step 1: Reliably determine financial currency from key-metrics
    let financialCurrency;
    if (metricsData && metricsData.reportedCurrency) {
      financialCurrency = metricsData.reportedCurrency;
    } else {
      financialCurrency = getCompanyCurrency(profileData);
      console.warn(
        `[DATA-CURRENCY] ${symbol}: Using profile currency '${financialCurrency}' as fallback.`,
      );
    }

    // Step 2: Determine market cap value with proper fallbacks
    let marketCapValue = 0;
    if (evData && evData.marketCapitalization > 0) {
      marketCapValue = evData.marketCapitalization;
    } else if (metricsData && metricsData.marketCap > 0) {
      console.warn(
        `[DATA-FALLBACK] ${symbol}: No 'enterprise-values' data, using 'key-metrics' market cap.`,
      );
      marketCapValue = metricsData.marketCap;
    } else if (profileData && profileData.mktCap > 0) {
      console.warn(
        `[DATA-FALLBACK] ${symbol}: No historical data, using LIVE 'profile' market cap.`,
      );
      marketCapValue = profileData.mktCap;
    }

    // --- END: CORRECTED LOGIC ---

    return {
      symbol: profileData.symbol,
      companyName: profileData.companyName,
      marketCap: marketCapValue,
      sector: profileData.sector,
      industry: getStandardizedIndustry(profileData.industry),
      country: profileData.country,
      currency: financialCurrency, // Use the CORRECT financial currency

      // --- START: NEW DUPLICATE HANDLING ---
      // Add the required fields for our de-duplication logic later.
      isActivelyTrading: profileData.isActivelyTrading,
      ipoDate: profileData.ipoDate,
      // --- END: NEW DUPLICATE HANDLING ---
    };
  } catch (e) {
    console.error(
      `[DATA] Error fetching basic data for ${symbol}: ${e.message}`,
    );
    return null;
  }
}

// =========================================================================
//  ADDITIONAL ENDPOINTS (/fetch-metric, /fetch-peers)
// =========================================================================

// =========================================================================
//  START: REPLACEMENT BLOCK FOR /fetch-metric
// =========================================================================
app.get("/fetch-metric", async (req, res) => {
    const { symbols, year, metric } = req.query;
    if (!symbols || !year || !metric) {
        return res.status(400).json({ error: "Missing parameters" });
    }

    const symbolList = symbols.split(",");
    const MAX_CONCURRENT = 10;
    let results = [];

    // This is the key: get the exchange rates for the requested year just ONCE.
    const rates = await getYearlyAverageUsdExchangeRates(year);

    for (let i = 0; i < symbolList.length; i += MAX_CONCURRENT) {
        const batch = symbolList.slice(i, i + MAX_CONCURRENT);
        // Pass the rates into the fetch function
        const promises = batch.map(s => fetchMetricData(s, year, metric, rates));
        results.push(...(await Promise.all(promises)));
        if (i + MAX_CONCURRENT < symbolList.length) await delay(200);
    }

    // Now, the 'results' array contains data that has already been normalized to USD.
    // We just need to format it for the response.
    const finalResults = results.filter(Boolean).map(result => {
        return {
            symbol: result.symbol,
            [metric]: result[metric], // This value is now in USD if it was a monetary metric
            currency: 'USD' // We explicitly state that the returned currency is USD
        };
    });

    res.json({
        companies: finalResults
    });
});
// =========================================================================
//  END: REPLACEMENT BLOCK FOR /fetch-metric
// =========================================================================


app.get("/fetch-peers", async (req, res) => {
  const { sector, year } = req.query;
  if (!sector || !year)
    return res.status(400).json({
      error: "Missing parameters",
    });
  const industry = getStandardizedIndustry(sector);

  console.log(
    `[FETCH-PEERS] Fetching peers for manual entry. Industry: ${industry}, Year: ${year}`,
  );

  const BENCHMARK_COUNTRY = "SA";

  const [localPeers, globalPeers] = await Promise.all([
    fetchSectorCompanies(industry, BENCHMARK_COUNTRY),
    fetchSectorCompanies(industry, null),
  ]);

  const screenerCompanyMap = new Map();
  const seenCompanyNames = new Set();

  [...localPeers, ...globalPeers].forEach((c) => {
    if (c && c.symbol && c.companyName) {
      if (
        !screenerCompanyMap.has(c.symbol) &&
        !seenCompanyNames.has(c.companyName)
      ) {
        screenerCompanyMap.set(c.symbol, c);
        seenCompanyNames.add(c.companyName);
      }
    }
  });

  const screenerCompanies = Array.from(screenerCompanyMap.values());
  console.log(
    `[FETCH-PEERS] Found ${screenerCompanies.length} unique potential peers for manual entry.`,
  );

  const verifiedPeers = await enrichCompaniesWithHistoricalMarketCap(
    screenerCompanies,
    year,
    10,
    500,
  );

  // Apply the same normalization logic here for accurate sorting
  const rates = await getYearlyAverageUsdExchangeRates(year);
  const normalizedPeers = verifiedPeers.map((company) => {
    const currency = (getCompanyCurrency(company) || "USD").toLowerCase();
    const rate = rates[currency];
    if (!rate) {
      console.warn(
        `[NORMALIZE-PEERS] No average exchange rate found for ${currency.toUpperCase()} for year ${year}. Market cap for ${company.symbol} might be inaccurate.`,
      );
      return { ...company, marketCapUSD: company.marketCap };
    }
    const marketCapUSD = (company.marketCap || 0) / rate;
    return { ...company, marketCapUSD };
  });

  const sorted = normalizedPeers.sort(
    (a, b) => (b.marketCapUSD || 0) - (a.marketCapUSD || 0),
  );

  // --- START: NEW DUPLICATE HANDLING ---
  // Also apply de-duplication here for consistency
  const deduplicatedPeers = deduplicateCompanies(sorted);
  // --- END: NEW DUPLICATE HANDLING ---

  const allCompaniesResponse = deduplicatedPeers.map((c) => ({
    ...c,
    isUserCompany: false,
  }));

  res.json({
    allCompanies: allCompaniesResponse,
    topCompanies: allCompaniesResponse.slice(0, 10),
    comparisonCompanies: allCompaniesResponse.slice(0, 20),
  });
});

/**
 * REVISED
 * This map now points to the new /stable/key-metrics and /stable/ratios endpoints.
 */
const fullMetricEndpointMap = {
  marketCap: { endpoint: "key-metrics", field: "marketCap" },
  enterpriseValue: { endpoint: "key-metrics", field: "enterpriseValue" },
  evToSales: { endpoint: "key-metrics", field: "evToSales" },
  enterpriseValueOverEBITDA: { endpoint: "key-metrics", field: "evToEBITDA" },
  evToOperatingCashFlow: {
    endpoint: "key-metrics",
    field: "evToOperatingCashFlow",
  },
  evToFreeCashFlow: { endpoint: "key-metrics", field: "evToFreeCashFlow" },
  netDebtToEBITDA: { endpoint: "key-metrics", field: "netDebtToEBITDA" },
  incomeQuality: { endpoint: "key-metrics", field: "incomeQuality" },
  grahamNumber: { endpoint: "key-metrics", field: "grahamNumber" },
  grahamNetNet: { endpoint: "key-metrics", field: "grahamNetNet" },
  roe: { endpoint: "key-metrics", field: "returnOnEquity" },
  returnOnEquity: { endpoint: "key-metrics", field: "returnOnEquity" },
  roic: { endpoint: "key-metrics", field: "returnOnInvestedCapital" },
  returnOnInvestedCapital: {
    endpoint: "key-metrics",
    field: "returnOnInvestedCapital",
  },
  returnOnTangibleAssets: {
    endpoint: "key-metrics",
    field: "returnOnTangibleAssets",
  },
  earningsYield: { endpoint: "key-metrics", field: "earningsYield" },
  freeCashFlowYield: { endpoint: "key-metrics", field: "freeCashFlowYield" },
  capexToOperatingCashFlow: {
    endpoint: "key-metrics",
    field: "capexToOperatingCashFlow",
  },
  capexToDepreciation: {
    endpoint: "key-metrics",
    field: "capexToDepreciation",
  },
  capexToRevenue: { endpoint: "key-metrics", field: "capexToRevenue" },
  salesGeneralAndAdministrativeToRevenue: {
    endpoint: "key-metrics",
    field: "salesGeneralAndAdministrativeToRevenue",
  },
  researchAndDdevelopementToRevenue: {
    endpoint: "key-metrics",
    field: "researchAndDevelopementToRevenue",
  },
  stockBasedCompensationToRevenue: {
    endpoint: "key-metrics",
    field: "stockBasedCompensationToRevenue",
  },
  intangiblesToTotalAssets: {
    endpoint: "key-metrics",
    field: "intangiblesToTotalAssets",
  },
  workingCapital: { endpoint: "key-metrics", field: "workingCapital" },
  investedCapital: { endpoint: "key-metrics", field: "investedCapital" },
  netCurrentAssetValue: {
    endpoint: "key-metrics",
    field: "netCurrentAssetValue",
  },
  averageReceivables: { endpoint: "key-metrics", field: "averageReceivables" },
  averagePayables: { endpoint: "key-metrics", field: "averagePayables" },
  averageInventory: { endpoint: "key-metrics", field: "averageInventory" },
  daysOfSalesOutstanding: {
    endpoint: "key-metrics",
    field: "daysOfSalesOutstanding",
  },
  daysOfPayablesOutstanding: {
    endpoint: "key-metrics",
    field: "daysOfPayablesOutstanding",
  },
  daysOfInventoryOutstanding: {
    endpoint: "key-metrics",
    field: "daysOfInventoryOutstanding",
  },
  operatingCycle: { endpoint: "key-metrics", field: "operatingCycle" },
  cashConversionCycle: {
    endpoint: "key-metrics",
    field: "cashConversionCycle",
  },
  grossProfitMargin: { endpoint: "ratios", field: "grossProfitMargin" },
  grossProfitRatio: { endpoint: "ratios", field: "grossProfitMargin" },
  ebitdaratio: { endpoint: "ratios", field: "ebitdaMargin" },
  ebitdaMargin: { endpoint: "ratios", field: "ebitdaMargin" },
  operatingIncomeRatio: { endpoint: "ratios", field: "operatingProfitMargin" },
  operatingProfitMargin: { endpoint: "ratios", field: "operatingProfitMargin" },
  incomeBeforeTaxRatio: { endpoint: "ratios", field: "pretaxProfitMargin" },
  pretaxProfitMargin: { endpoint: "ratios", field: "pretaxProfitMargin" },
  netIncomeRatio: { endpoint: "ratios", field: "netProfitMargin" },
  netProfitMargin: { endpoint: "ratios", field: "netProfitMargin" },
  currentRatio: { endpoint: "ratios", field: "currentRatio" },
  quickRatio: { endpoint: "ratios", field: "quickRatio" },
  cashRatio: { endpoint: "ratios", field: "cashRatio" },
  returnOnAssets: { endpoint: "ratios", field: "returnOnAssets" },
  returnOnCapitalEmployed: {
    endpoint: "ratios",
    field: "returnOnCapitalEmployed",
  },
  netIncomePerEBT: { endpoint: "ratios", field: "netIncomePerEBT" },
  ebtPerEbit: { endpoint: "ratios", field: "ebtPerEbit" },
  debtRatio: { endpoint: "ratios", field: "debtToAssetsRatio" },
  debtToAssets: { endpoint: "ratios", field: "debtToAssetsRatio" },
  debtEquityRatio: { endpoint: "ratios", field: "debtToEquityRatio" },
  longTermDebtToCapitalization: {
    endpoint: "ratios",
    field: "longTermDebtToCapitalRatio",
  },
  totalDebtToCapitalization: {
    endpoint: "ratios",
    field: "debtToCapitalRatio",
  },
  interestCoverage: { endpoint: "ratios", field: "interestCoverageRatio" },
  cashFlowToDebtRatio: { endpoint: "ratios", field: "cashFlowToDebtRatio" },
  companyEquityMultiplier: {
    endpoint: "ratios",
    field: "financialLeverageRatio",
  },
  receivablesTurnover: { endpoint: "ratios", field: "receivablesTurnover" },
  payablesTurnover: { endpoint: "ratios", field: "payablesTurnover" },
  inventoryTurnover: { endpoint: "ratios", field: "inventoryTurnover" },
  fixedAssetTurnover: { endpoint: "ratios", field: "fixedAssetTurnover" },
  assetTurnover: { endpoint: "ratios", field: "assetTurnover" },
  operatingCashFlowPerShare: {
    endpoint: "ratios",
    field: "operatingCashFlowPerShare",
  },
  freeCashFlowPerShare: { endpoint: "ratios", field: "freeCashFlowPerShare" },
  cashPerShare: { endpoint: "ratios", field: "cashPerShare" },
  payoutRatio: { endpoint: "ratios", field: "dividendPayoutRatio" },
  operatingCashFlowSalesRatio: {
    endpoint: "ratios",
    field: "operatingCashFlowSalesRatio",
  },
  freeCashFlowOperatingCashFlowRatio: {
    endpoint: "ratios",
    field: "freeCashFlowOperatingCashFlowRatio",
  },
  cashFlowCoverageRatios: {
    endpoint: "ratios",
    field: "operatingCashFlowCoverageRatio",
  },
  shortTermCoverageRatios: {
    endpoint: "ratios",
    field: "shortTermOperatingCashFlowCoverageRatio",
  },
  capitalExpenditureCoverageRatio: {
    endpoint: "ratios",
    field: "capitalExpenditureCoverageRatio",
  },
  dividendPaidAndCapexCoverageRatio: {
    endpoint: "ratios",
    field: "dividendPaidAndCapexCoverageRatio",
  },
  dividendPayoutRatio: { endpoint: "ratios", field: "dividendPayoutRatio" },
  priceBookValueRatio: { endpoint: "ratios", field: "priceToBookRatio" },
  priceToBookRatio: { endpoint: "ratios", field: "priceToBookRatio" },
  ptbRatio: { endpoint: "ratios", field: "priceToBookRatio" },
  priceToSalesRatio: { endpoint: "ratios", field: "priceToSalesRatio" },
  priceSalesRatio: { endpoint: "ratios", field: "priceToSalesRatio" },
  priceEarningsRatio: { endpoint: "ratios", field: "priceToEarningsRatio" },
  peRatio: { endpoint: "ratios", field: "priceToEarningsRatio" },
  priceToFreeCashFlowsRatio: {
    endpoint: "ratios",
    field: "priceToFreeCashFlowRatio",
  },
  pfcfRatio: { endpoint: "ratios", field: "priceToFreeCashFlowRatio" },
  priceToOperatingCashFlowsRatio: {
    endpoint: "ratios",
    field: "priceToOperatingCashFlowRatio",
  },
  pocfratio: { endpoint: "ratios", field: "priceToOperatingCashFlowRatio" },
  priceCashFlowRatio: {
    endpoint: "ratios",
    field: "priceToOperatingCashFlowRatio",
  },
  priceEarningsToGrowthRatio: {
    endpoint: "ratios",
    field: "priceToEarningsGrowthRatio",
  },
  dividendYield: { endpoint: "ratios", field: "dividendYield" },
  enterpriseValueMultiple: {
    endpoint: "ratios",
    field: "enterpriseValueMultiple",
  },
  priceFairValue: { endpoint: "ratios", field: "priceToFairValue" },
  revenuePerShare: { endpoint: "ratios", field: "revenuePerShare" },
  netIncomePerShare: { endpoint: "ratios", field: "netIncomePerShare" },
  bookValuePerShare: { endpoint: "ratios", field: "bookValuePerShare" },
  tangibleBookValuePerShare: {
    endpoint: "ratios",
    field: "tangibleBookValuePerShare",
  },
  shareholdersEquityPerShare: {
    endpoint: "ratios",
    field: "shareholdersEquityPerShare",
  },
  interestDebtPerShare: { endpoint: "ratios", field: "interestDebtPerShare" },
  capexPerShare: { endpoint: "ratios", field: "capexPerShare" },
  effectiveTaxRate: { endpoint: "ratios", field: "effectiveTaxRate" },
  revenue: { endpoint: "income-statement", field: "revenue" },
  growthRevenue: {
    endpoint: "income-statement-growth",
    field: "growthRevenue",
  },
  growthGrossProfitRatio: {
    endpoint: "income-statement-growth",
    field: "growthGrossProfit",
  },
  growthEBITDARatio: {
    endpoint: "income-statement-growth",
    field: "growthEBITDA",
  },
  growthOperatingIncomeRatio: {
    endpoint: "income-statement-growth",
    field: "growthOperatingIncome",
  },
  growthIncomeBeforeTaxRatio: {
    endpoint: "income-statement-growth",
    field: "growthIncomeBeforeTax",
  },
  growthNetIncomeRatio: {
    endpoint: "income-statement-growth",
    field: "growthNetIncome",
  },
};

// =========================================================================
//  START: REPLACEMENT BLOCK FOR fetchMetricData
// =========================================================================
// This helper function now checks if a metric is monetary
function isMonetaryMetric(metric) {
    const monetaryMetrics = [
        'marketCap', 'enterpriseValue', 'grahamNumber', 'grahamNetNet',
        'workingCapital', 'investedCapital', 'netCurrentAssetValue',
        'averageReceivables', 'averagePayables', 'averageInventory',
        'revenue', 'revenuePerShare', 'netIncomePerShare',
        'bookValuePerShare', 'tangibleBookValuePerShare',
        'shareholdersEquityPerShare', 'interestDebtPerShare',
        'capexPerShare', 'operatingCashFlowPerShare', 'freeCashFlowPerShare',
        'cashPerShare'
    ];
    return monetaryMetrics.includes(metric);
}


async function fetchMetricData(symbol, year, metric, rates) { // It now accepts 'rates'
    // The special case for 'revenuePerFte' remains the same as it has its own logic
    if (metric === "revenuePerFte") {
        console.log(
          `[METRIC] Calculating special metric 'revenuePerFte' for ${symbol}`,
        );
        try {
          const [incomeStatementData, profileDataRes] = await Promise.all([
            getCachedAnnualData(symbol, "income-statement"),
            getCachedAnnualData(symbol, "profile"),
          ]);
    
          let revenueYearData = Array.isArray(incomeStatementData)
            ? incomeStatementData.find((d) => String(d.fiscalYear) === String(year))
            : null;
    
          if (
            !revenueYearData &&
            Array.isArray(incomeStatementData) &&
            incomeStatementData.length > 0
          ) {
            console.warn(
              `[METRIC-FALLBACK] No revenue for year ${year} for ${symbol}. Using most recent available.`,
            );
            incomeStatementData.sort(
              (a, b) => parseInt(b.fiscalYear) - parseInt(a.fiscalYear),
            );
            revenueYearData = incomeStatementData[0];
          }
    
          const profileData = Array.isArray(profileDataRes)
            ? profileDataRes[0]
            : null;
    
          if (
            revenueYearData &&
            revenueYearData.revenue != null &&
            profileData &&
            profileData.fullTimeEmployees > 0
          ) {
            const revenue = revenueYearData.revenue;
            const employees = parseInt(profileData.fullTimeEmployees, 10);
    
            if (isNaN(employees) || employees <= 0) {
              console.warn(
                `[METRIC] Invalid employee count for ${symbol}: '${profileData.fullTimeEmployees}'`,
              );
              return { symbol, [metric]: "N/A" };
            }
            
            // This is a special case. We will normalize it here as well.
            let calculatedValue = revenue / employees;
            const originalCurrency = getCompanyCurrency(revenueYearData).toLowerCase();
            if (originalCurrency !== 'usd') {
                const conversionRate = rates[originalCurrency];
                if(conversionRate) {
                    calculatedValue = calculatedValue / conversionRate;
                } else {
                    calculatedValue = 'N/A';
                }
            }
            return { symbol, [metric]: calculatedValue };
          } else {
            let reason = "Required data not available.";
            if (!revenueYearData || revenueYearData.revenue == null) {
              reason = `Revenue for year ${year} (or fallback) not found.`;
            } else if (!profileData || !profileData.fullTimeEmployees) {
              reason = "Full Time Employees not found in company profile.";
            } else if (parseInt(profileData.fullTimeEmployees, 10) <= 0) {
              reason = "Full Time Employees is zero or invalid in profile.";
            }
            console.warn(
              `[METRIC] Could not calculate 'revenuePerFte' for ${symbol}. Reason: ${reason}`,
            );
            return { symbol, [metric]: "N/A" };
          }
        } catch (e) {
          console.error(
            `[METRIC] Unhandled exception fetching data for 'revenuePerFte' for ${symbol}: ${e.message}`,
          );
          return { symbol, [metric]: "N/A" };
        }
    }

    const info = fullMetricEndpointMap[metric];
    if (!info) {
        console.warn(`[METRIC] No mapping found for metric: ${metric}`);
        return null;
    }

    try {
        const endpointData = await getCachedAnnualData(symbol, info.endpoint);
        const yearData = Array.isArray(endpointData) ? endpointData.find(d => String(d.fiscalYear) === String(year)) : null;

        if (!yearData || yearData[info.field] == null) {
            return { symbol, [metric]: 'N/A' };
        }

        let finalValue = yearData[info.field];

        // +++ THIS IS THE NORMALIZATION LOGIC WE ARE ADDING +++
        // Check if the metric is monetary and needs conversion
        if (isMonetaryMetric(metric)) {
            const originalCurrency = getCompanyCurrency(yearData).toLowerCase();
            if (originalCurrency !== 'usd') {
                const conversionRate = rates[originalCurrency];
                if (conversionRate) {
                    // Divide to convert the foreign currency value TO USD
                    finalValue = finalValue / conversionRate;
                } else {
                    console.warn(`[METRIC-CONVERT] No rate for ${originalCurrency.toUpperCase()}, cannot convert ${metric} for ${symbol}.`);
                    finalValue = 'N/A';
                }
            }
        }
        // +++ END OF NEW LOGIC +++

        return {
            symbol,
            [metric]: finalValue // This is now the USD-normalized value
        };

    } catch (e) {
        console.error(`[METRIC] Failed to fetch ${metric} for ${symbol}: ${e.message}`);
        return {
            symbol,
            [metric]: 'N/A'
        };
    }
}
// =========================================================================
//  END: REPLACEMENT BLOCK FOR fetchMetricData
// =========================================================================


// =========================================================================
//  SERVER START
// =========================================================================

// === ADD THIS ONE ROUTE FOR UPTIMEROBOT ===
app.get("/", (req, res) => {
  res.send("Server is online and ready.");
});
// ==========================================

const listener = app.listen(process.env.PORT || 3000, "0.0.0.0", () => {
  const port = listener.address().port;
  console.log(`Server is listening on port ${port}`);
  console.log(`Your app is listening on http://localhost:${port}`);
  if (process.env.PROJECT_DOMAIN) {
    console.log(
      `Server is listening on https://${process.env.PROJECT_DOMAIN}.glitch.me`,
    );
  }
});
