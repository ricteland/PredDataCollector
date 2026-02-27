const fs = require('fs');
const path = require('path');

const API_URL = 'https://gamma-api.polymarket.com/events';

const getExpected1hSlugs = (coinName) => {
    const slugs = [];
    const nowLocal = new Date();
    for (let i = 0; i < 4; i++) {
        const d = new Date(nowLocal.getTime() + (i * 60 * 60 * 1000));

        const formatter = new Intl.DateTimeFormat('en-US', {
            timeZone: 'America/New_York',
            month: 'long',
            day: 'numeric',
            hour: 'numeric',
            hour12: true
        });

        const parts = formatter.formatToParts(d);
        const month = parts.find(p => p.type === 'month').value.toLowerCase();
        const day = parts.find(p => p.type === 'day').value;
        const hour = parts.find(p => p.type === 'hour').value;
        const dayPeriod = parts.find(p => p.type === 'dayPeriod').value.toLowerCase().replace(/\s+/g, '');

        slugs.push(`${coinName}-up-or-down-${month}-${day}-${hour}${dayPeriod}-et`);
    }
    return slugs;
};

const fetchEvents = async () => {
    console.log('Fetching active events broadly to map all slugs...');
    let allData = [];
    let offset = 0;
    let hasMore = true;

    // Fetch all active markets using limit=100 (Gamma API bugs out and caps results if limit=1000 is used)
    while (hasMore) {
        try {
            const res = await fetch(`${API_URL}?active=true&closed=false&limit=100&offset=${offset}`);
            const data = await res.json();
            if (!data || data.length === 0) {
                hasMore = false;
            } else {
                allData = [...allData, ...data];
                offset += 100;
            }
        } catch (e) {
            console.error('Fetch error:', e);
            break;
        }
    }

    const outputData = {
        discovered_at: new Date().toISOString(),
        source: 'Polymarket Event Fetcher (Active Snapshot)',
        markets: { BTC: {}, ETH: {}, SOL: {}, XRP: {} }
    };

    const coins = ['btc', 'eth'];
    for (const coin of coins) {
        outputData.markets[coin.toUpperCase()] = {
            '1h': { series_id: '', series_slug: '', events: [] },
            '15m': { series_id: '', series_slug: '', events: [] },
            '5m': { series_id: '', series_slug: '', events: [] }
        };
    }

    const longNames = { btc: 'bitcoin', eth: 'ethereum', sol: 'solana', xrp: 'xrp' };

    for (const coin of coins) {
        const longName = longNames[coin];

        // Exact slug matching for 1h sequentially mapped to ET time
        const expected1hSlugs = getExpected1hSlugs(longName);
        const events1h = allData.filter(e => e.slug && expected1hSlugs.includes(e.slug));

        // Broad includes for 15m and 5m
        const events15m = allData.filter(e => e.slug && e.slug.includes(`${coin}-updown-15m`));
        const events5m = allData.filter(e => e.slug && e.slug.includes(`${coin}-updown-5m`));

        const processSubset = (subset, is1h) => {
            let res = [];
            let series_id = '';
            let series_slug = '';

            const now = new Date();
            const threeHoursLater = new Date(now.getTime() + (3 * 60 * 60 * 1000));

            for (const event of subset) {
                const eventEndDateStr = event.endDate || event.end_date;
                if (!eventEndDateStr) continue;

                const eventEndDate = new Date(eventEndDateStr);

                // Skip past markets
                if (eventEndDate <= now) {
                    continue;
                }

                // For 15m and 5m, apply the 3-hour strict ceiling
                // (1h is already strictly constrained by the 4 explicitly generated ET slugs)
                if (!is1h && eventEndDate > threeHoursLater) {
                    continue;
                }

                const activeMarkets = event.markets ? event.markets.filter(m => m.active) : [];
                const market = activeMarkets.length > 0 ? activeMarkets[0] : (event.markets && event.markets.length > 0 ? event.markets[0] : null);
                if (!market) continue;

                // Get the series details if we haven't
                if (!series_id && event.series_id) {
                    series_id = String(event.series_id);
                }
                if (!series_slug && event.tags && event.tags.length > 0) {
                    const tag = event.tags.find(t => t.slug);
                    if (tag) series_slug = tag;
                }

                let outcomes = [];
                let prices = [];
                let clobTokenIds = [];

                try {
                    outcomes = JSON.parse(market.outcomes || '[]');
                    prices = JSON.parse(market.outcomePrices || '[]');
                    clobTokenIds = JSON.parse(market.clobTokenIds || '[]');
                } catch (e) {
                    continue;
                }

                if (outcomes.length < 2 || prices.length < 2 || clobTokenIds.length < 2) continue;

                const tokensDict = {};
                for (let i = 0; i < outcomes.length; i++) {
                    const key = outcomes[i].toLowerCase();
                    tokensDict[key] = {
                        token_id: clobTokenIds[i],
                        outcome: outcomes[i],
                        price: String(prices[i])
                    };
                }

                res.push({
                    event_slug: event.slug,
                    condition_id: event.condition_id || market.conditionId || market.condition_id,
                    end_date: event.endDate || event.end_date,
                    tokens: tokensDict
                });
            }
            res.sort((a, b) => new Date(a.end_date) - new Date(b.end_date));
            return { series_id, series_slug, events: res };
        };

        outputData.markets[coin.toUpperCase()]['1h'] = processSubset(events1h, true);
        outputData.markets[coin.toUpperCase()]['15m'] = processSubset(events15m, false);
        outputData.markets[coin.toUpperCase()]['5m'] = processSubset(events5m, false);
    }

    const outPath = path.join(__dirname, 'polymarket_data_fetched.json');
    fs.writeFileSync(outPath, JSON.stringify(outputData, null, 2));
    console.log(`✅ Success! Data perfectly extracted and saved to ${outPath}`);
};

fetchEvents().catch(console.error);
