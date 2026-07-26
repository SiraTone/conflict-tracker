WIRE // Global Conflict Monitor — advanced OSINT dashboard (standalone app).

Legal, public-data-only OSINT aggregator. Everything here pulls from open
RSS feeds, public subreddit RSS, public Telegram channel preview pages,
public prediction markets, public ADS-B data, public USGS seismic data,
and free public market-price data. No private/individual tracking, no
image geolocation, no targeting features of any kind.

Panels:
  - Wire feed: keyword-filtered headlines from a large set of international
    + regional news RSS feeds, Gulf/Levant subreddits, and public Telegram
    channels, each polling independently.
  - Intensity graph: rolling event-volume-per-hour chart, split by source
    type, so spikes in reporting are visible at a glance.
  - Geo panel: lightweight gazetteer match against headline text, plotting
    country/city-level mentions on a simple world map. Country/city level
    only — this is a "where is activity clustering" view, not a
    geolocation tool for people or individual images.
  - Confidence scoring: alert-tier items are cross-referenced against how
    many distinct sources reported something matching the same keyword
    cluster within a rolling window, giving a rough corroboration score
    instead of a single binary "alert" flag.
  - Markets strip: live Polymarket odds for conflict-relevant questions.
  - Commodities/FX strip: public price data (oil, gold, key regional FX)
    since markets often price in escalation risk before headlines catch up.
  - Seismic cross-reference: public USGS feed, flagged only as "worth a
    look" near conflict zones — many quakes are just quakes.
  - Flights strip: best-effort CENTCOM-AOR ADS-B (civilian-visible only).
  - Timeline/history log: persistent (session-lifetime) event log you can
    scroll back through, separate from the live-scrolling wire.

Run it:
    pip install flask feedparser tzdata requests beautifulsoup4
    python app.py
