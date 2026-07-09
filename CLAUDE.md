# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Scraping service for the kijiji-map app, deployed as a DigitalOcean Serverless Function (`project.yml` defines the `scraper/scrape` function, Node.js 18). It runs hourly via a scheduler trigger (`hourly-scrape-trigger`, cron `0 * * * *`) — it is not a web function.

The entire implementation is `packages/scraper/scrape/index.js` (the `main` export is the function entry point).

## Commands

- Deploy: `doctl serverless deploy .` (from the repo root; requires `doctl` connected to the DigitalOcean Functions namespace)
- Install function dependencies: `npm install` inside `packages/scraper/scrape/`
- There are no tests or linting configured.

## How the scraper works

One run of `main()` does the following, in order:

1. Fetches existing listing IDs from the `listing-features` MongoDB collection to dedupe against.
2. Scrapes the Kijiji Toronto apartments/condos search page (sorted by date) with `fetch` + cheerio, extracting ad links via the `[data-testid="listing-link"]` selector.
3. Fetches each new ad's page and parses the `__NEXT_DATA__` script tag: the listing lives in the Apollo GraphQL cache (`props.pageProps.__APOLLO_STATE__`) under a `RealEstateListing:<id>` key. Prices there are in cents (the DB stores dollars), and `numberbathrooms` canonical values are scaled by 10 ("15" = 1.5 baths). Both the search-page selector and this structure are coupled to Kijiji's markup — they are the likely breakage points if scraping stops working.
4. Maps each ad to a GeoJSON `Feature` (Point geometry from the ad's lat/long, listing details under `properties`).
5. Inserts into the `pending-listings` collection, then runs a `$merge` aggregation into `listing-features` (matched on `properties.listingId`, `keepExisting` on conflict), then clears `pending-listings`.

## Configuration

MongoDB Atlas credentials come from environment variables `DB_USER`, `DB_PASSWORD`, and `DB_URI`, which `project.yml` injects into the function from a local `.env` file (gitignored). The database name is `kijiji-map`.

`HEALTHCHECK_PING_URL` (also injected from `.env`) is a healthchecks.io ping URL used as a dead-man's switch: each run pings it on success or `<url>/fail` on failure, and a missed hourly ping triggers an alert. Every variable referenced in `project.yml` must be present in `.env` or `doctl serverless deploy` fails.

## Failure monitoring

- A run that finds 0 ads on the search page throws (the selector broke) rather than exiting successfully.
- Errors are rethrown from `main()` after pinging the healthcheck, so failed runs show up in DigitalOcean's Functions insights (error rate, last 14 days) and `doctl serverless activations list`. Function logs are retained for 3 days.
